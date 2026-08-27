using PlatformService.Dtos;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Text;
using System.Text.Json;

namespace PlatformService.AsyncDataServices
{
    public class MessageBusClient : IAsyncDisposable // IMessageBusClient, 
    {
        private readonly ConnectionFactory _factory;

        private IConnection? _connection;
        private IChannel? _channel;

        private const string ExchangeName = "trigger";

        // Initial connection retry policy
        private const int MaxRetryAttempts = 5;
        private const int MaxRetryDelaySeconds = 30;

        private readonly CancellationTokenSource _shutdownCts = new();

        /*
         * RabbitMQ recommends avoiding concurrent publishing
         * on the same IChannel.
         *
         * This lock also ensures that connection/channel creation
         * and publishing cannot interfere with each other.
         */
        private readonly SemaphoreSlim _publishLock = new(1, 1);

        public MessageBusClient(IConfiguration configuration)
        {
            _factory = new ConnectionFactory
            {
                HostName = configuration["RabbitMQ:Host"]
                    ?? throw new InvalidOperationException(
                        "RabbitMQ:Host is not configured."),

                Port = int.Parse(configuration["RabbitMQ:Port"] ?? "5672"),

                /*
                 * This is for recovery AFTER a connection
                 * has already been successfully established.
                 */
                AutomaticRecoveryEnabled = true,

                NetworkRecoveryInterval = TimeSpan.FromSeconds(5),

                ClientProvidedName = "platform-service-publisher"
            };
        }

        /*
         * Makes sure we have a usable RabbitMQ connection/channel.
         *
         * IMPORTANT:
         * - Initial connection failure -> our bounded retry logic.
         * - Existing connection lost -> RabbitMQ automatic recovery.
         */
        private async Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken)
        {
            // Everything is healthy.
            if (_connection?.IsOpen == true && _channel?.IsOpen == true)
            {
                return true;
            }

            /*
             * Connection is healthy but channel is closed.
             *
             * Channel-level failures do not necessarily mean that
             * the TCP/AMQP connection itself is bad, so recreate
             * only the channel.
             */
            if (_connection?.IsOpen == true)
            {
                Console.WriteLine( "--> RabbitMQ connection is open but channel is closed. Recreating channel...");

                await CleanupChannelAsync();

                try
                {
                    await CreateChannelAsync(cancellationToken);

                    Console.WriteLine("--> RabbitMQ channel recreated.");

                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine( $"--> Could not recreate RabbitMQ channel: {ex.Message}");

                    await CleanupChannelAsync();

                    return false;
                }
            }

            /*
             * We HAD a successfully-established connection,
             * but it is currently down.
             *
             * AutomaticRecoveryEnabled is responsible for recovering it.
             * Do not create another competing connection here.
             */
            if (_connection != null)
            {
                Console.WriteLine("--> RabbitMQ connection is currently recovering.");

                return false;
            }

            /*
             * No connection exists.
             *
             * This means we need an initial connection.
             * Use bounded retry with exponential backoff.
             */
            return await ConnectWithRetryAsync(cancellationToken);
        }

        /*
         * Initial RabbitMQ connection.
         *
         * Maximum 5 attempts.
         *
         * Delays:
         * attempt 1 fails -> wait 2 sec
         * attempt 2 fails -> wait 4 sec
         * attempt 3 fails -> wait 8 sec
         * attempt 4 fails -> wait 16 sec
         * attempt 5 fails -> STOP
         *
         * Total retry delay = 30 seconds.
         */
        private async Task<bool> ConnectWithRetryAsync(
            CancellationToken cancellationToken)
        {
            for (int attempt = 1;
                 attempt <= MaxRetryAttempts;
                 attempt++)
            {
                try
                {
                    Console.WriteLine($"--> Connecting to RabbitMQ. " +
                        $"Attempt {attempt}/{MaxRetryAttempts}...");

                    // 1. Create TCP/AMQP connection
                    _connection =
                        await _factory.CreateConnectionAsync(
                            cancellationToken);

                    // 2. Register shutdown handler
                    _connection.ConnectionShutdownAsync += RabbitMQ_ConnectionShutdown;

                    // 3. Create channel and declare exchange
                    await CreateChannelAsync(cancellationToken);

                    Console.WriteLine("--> Connected to RabbitMQ Message Bus!");

                    return true;
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    return false;
                }
                catch (BrokerUnreachableException ex)
                {
                    Console.WriteLine( $"--> RabbitMQ unavailable: {ex.Message}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"--> RabbitMQ connection failed: {ex.Message}");
                }

                // Clean up any partially-created resources
                await CleanupRabbitMqAsync();

                /*
                 * Do not wait after the final attempt.
                 */
                if (attempt == MaxRetryAttempts)
                {
                    Console.WriteLine( $"--> RabbitMQ connection failed after " +
                        $"{MaxRetryAttempts} attempts.");

                    return false;
                }

                /*
                 * Exponential backoff:
                 *
                 * 2, 4, 8, 16...
                 */
                var delaySeconds = Math.Min(
                    (int)Math.Pow(2, attempt),
                    MaxRetryDelaySeconds);

                Console.WriteLine(
                    $"--> Retrying RabbitMQ in " +
                    $"{delaySeconds} seconds...");

                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(delaySeconds),
                        cancellationToken);
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    return false;
                }
            }

            return false;
        }

        /*
         * Creates a channel and declares the exchange.
         */
        private async Task CreateChannelAsync(CancellationToken cancellationToken)
        {
            if (_connection == null ||
                !_connection.IsOpen)
            {
                throw new InvalidOperationException("Cannot create RabbitMQ channel because the connection is not open.");
            }

            _channel =
                await _connection.CreateChannelAsync(cancellationToken: cancellationToken);

            await _channel.ExchangeDeclareAsync(
                exchange: ExchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);
        }

        public async Task PublishNewPlatformAsync(PlatformPublishedDto platformPublishedDto, string correlationId, CancellationToken cancellationToken = default)
        {
            /*
             * One publisher at a time because the same IChannel
             * is shared by this singleton.
             */
            await _publishLock.WaitAsync(cancellationToken);

            try
            {
                var connected = await EnsureConnectedAsync(cancellationToken);

                if (!connected)
                {
                    /*
                     * DO NOT silently discard the message.
                     *
                     * For now we surface the failure.
                     *
                     * Later publisher confirms + retry/outbox
                     * will give us a stronger solution.
                     */
                    throw new InvalidOperationException(
                        "RabbitMQ is unavailable or recovering. " +
                        "The platform event was not published.");
                }

                /*
                 * Check again immediately before publishing.
                 *
                 * This still cannot guarantee delivery:
                 * the connection can fail between this check
                 * and BasicPublishAsync().
                 *
                 * Publisher confirms will address that next.
                 */
                if (_channel == null ||
                    !_channel.IsOpen)
                {
                    throw new InvalidOperationException("RabbitMQ channel is not open. " +
                        "The platform event was not published.");
                }

                var message =
                    JsonSerializer.Serialize(platformPublishedDto);

                var body =
                    Encoding.UTF8.GetBytes(message);

                await _channel.BasicPublishAsync(
                    exchange: ExchangeName,
                    routingKey: string.Empty,
                    body: body,
                    cancellationToken: cancellationToken);

                Console.WriteLine("--> New platform published");
            }
            finally
            {
                _publishLock.Release();
            }
        }

        private Task RabbitMQ_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            Console.WriteLine(
                $"--> RabbitMQ connection shutdown: {e.ReplyText}");

            return Task.CompletedTask;
        }

        /*
         * Clean up only the channel.
         */
        private async Task CleanupChannelAsync()
        {
            if (_channel == null)
            {
                return;
            }

            try
            {
                if (_channel.IsOpen)
                {
                    await _channel.CloseAsync();
                }

                await _channel.DisposeAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"--> Error cleaning RabbitMQ channel: {ex.Message}");
            }
            finally
            {
                _channel = null;
            }
        }

        /*
         * Clean up channel + connection.
         *
         * Used for:
         * - failed initial connection attempts
         * - application shutdown
         */
        private async Task CleanupRabbitMqAsync()
        {
            await CleanupChannelAsync();

            if (_connection == null)
            {
                return;
            }

            try
            {
                _connection.ConnectionShutdownAsync -= RabbitMQ_ConnectionShutdown;

                if (_connection.IsOpen)
                {
                    await _connection.CloseAsync();
                }

                await _connection.DisposeAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"--> Error cleaning RabbitMQ connection: {ex.Message}");
            }
            finally
            {
                _connection = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            Console.WriteLine("--> Disposing MessageBusClient");

            /*
             * Stop any retry/delay/publish operation.
             */
            await _shutdownCts.CancelAsync();

            /*
             * Wait until any current publish/connection operation
             * has finished or reacted to cancellation.
             */
            await _publishLock.WaitAsync();

            try
            {
                await CleanupRabbitMqAsync();
            }
            finally
            {
                _publishLock.Release();
            }

            _publishLock.Dispose();
            _shutdownCts.Dispose();

            Console.WriteLine("--> MessageBusClient disposed");
        }
    }
}