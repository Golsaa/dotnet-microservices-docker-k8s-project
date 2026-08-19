using PlatformService.Dtos;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace PlatformService.AsyncDataServices
{
    public class MessageBusClient : IMessageBusClient, IAsyncDisposable
    {
        private readonly IConfiguration _configuration;

        private IConnection? _connection;
        private IChannel? _channel;

        private const string ExchangeName = "trigger";
        private readonly Task _initializationTask;
        
        //RabbitMQ specifically warns that an IChannel should not be used concurrently by multiple threads for publishing.
        //For your learning project, the simplest safe solution is to protect publishing with a SemaphoreSlim:
        private readonly SemaphoreSlim _publishLock = new(1, 1);

        public MessageBusClient(IConfiguration configuration)
        {
            _configuration = configuration;
            _initializationTask = InitializeAsync();
        }

        private async Task InitializeAsync()
        {
            var factory = new ConnectionFactory
            {
                HostName = _configuration["RabbitMQ:Host"]
                    ?? throw new InvalidOperationException(
                        "RabbitMQ:Host is not configured."),

                Port = int.Parse(
                    _configuration["RabbitMQ:Port"] ?? "5672"),

                AutomaticRecoveryEnabled = true
            };

            try
            {
                // 1. Create TCP/AMQP connection
                _connection = await factory.CreateConnectionAsync();

                // 2. Create AMQP channel
                _channel = await _connection.CreateChannelAsync();

                // 3. Declare exchange
                await _channel.ExchangeDeclareAsync(
                    exchange: ExchangeName,
                    type: ExchangeType.Fanout,
                    durable: true,
                    autoDelete: false);

                // 4. Register connection shutdown handler
                _connection.ConnectionShutdownAsync += RabbitMQ_ConnectionShutdown;

                Console.WriteLine("--> Connected to RabbitMQ Message Bus!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"--> Could not connect to the Message Bus: {ex.Message}");
            }
        }

        public async Task PublishNewPlatformAsync(
    PlatformPublishedDto platformPublishedDto)
        {
            await _initializationTask;

            if (_channel == null || !_channel.IsOpen)
            {
                Console.WriteLine("--> RabbitMQ channel is not open");
                return;
            }

            await _publishLock.WaitAsync();

            try
            {
                var message = JsonSerializer.Serialize(platformPublishedDto);
                var body = Encoding.UTF8.GetBytes(message);

                await _channel.BasicPublishAsync(
                    exchange: "trigger",
                    routingKey: string.Empty,
                    body: body);

                Console.WriteLine("--> New platform published");
            }
            finally
            {
                _publishLock.Release();
            }
        }
     


        private Task RabbitMQ_ConnectionShutdown(object sender,
            ShutdownEventArgs e)
        {
            Console.WriteLine($"--> RabbitMQ connection shutdown: {e.ReplyText}");

            return Task.CompletedTask;
        }

        public async ValueTask DisposeAsync()
        {
            Console.WriteLine("--> Disposing MessageBusClient");

            if (_channel != null)
            {
                if (_channel.IsOpen)
                {
                    await _channel.CloseAsync();
                }

                await _channel.DisposeAsync();
            }

            if (_connection != null)
            {
                if (_connection.IsOpen)
                {
                    await _connection.CloseAsync();
                }

                await _connection.DisposeAsync();
            }

            Console.WriteLine("--> MessageBusClient disposed");
        }
    }
}