using System.Text.Json;
using CommandsService.EventProcessing;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace CommandsService.AsyncDataServices
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<KafkaConsumerService> _logger;

        private CircuitState _circuitState = CircuitState.Closed;

        private DateTimeOffset _circuitOpenedAtUtc;

        public KafkaConsumerService(
            IConfiguration configuration,
            IServiceScopeFactory scopeFactory,
            ILogger<KafkaConsumerService> logger)
        {
            _configuration = configuration;
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => ConsumeAsync(stoppingToken), stoppingToken);
        }

        private void Consume1(CancellationToken stoppingToken)
        {
            var bootstrapServers = _configuration["Kafka:BootstrapServers"]?? throw new InvalidOperationException("Kafka BootstrapServers is missing.");

            var topic = _configuration["Kafka:Topic"] ?? "platform-published";

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "commands-service",
                AutoOffsetReset = AutoOffsetReset.Earliest,

                // Important: we'll commit only after successful processing
                EnableAutoCommit = false
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe(topic);

            _logger.LogInformation("Kafka consumer subscribed to {Topic}", topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result = consumer.Consume(stoppingToken);

                    try
                    {
                        _logger.LogInformation("Kafka message received Partition={Partition} Offset={Offset}",
                            result.Partition, result.Offset);

                        using var scope = _scopeFactory.CreateScope();

                        var eventProcessor = scope.ServiceProvider.GetRequiredService<IEventProcessor>();

                        // Reuse your EXISTING event processing logic.
                        eventProcessor.ProcessEvent(result.Message.Value);

                        // Only commit after processing succeeded.
                        consumer.Commit(result);

                        _logger.LogInformation("Kafka offset committed: {Offset}",
                            result.Offset);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing Kafka event.");

                        // Do NOT commit.
                        // Later we'll add:
                        // retry
                        // exponential backoff
                        // DLQ
                        // circuit breaker
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // normal application shutdown
            }
            finally
            {
                consumer.Close();
            }
        }
    

    //####################################
        private async Task ConsumeAsync(CancellationToken stoppingToken)
        {
            var bootstrapServers =_configuration["Kafka:BootstrapServers"]
                ?? throw new InvalidOperationException("Kafka BootstrapServers is missing.");

            var topic = _configuration["Kafka:Topic"]
                ?? "platform-published";

            var dlqTopic = _configuration["Kafka:DlqTopic"]
                ?? "platform-published-dlq";

            var circuitOpenSeconds = GetIntConfiguration(
                    "Kafka:CircuitBreaker:OpenSeconds", 30);

            // ----------------------------------------------------
            // Consumer configuration
            // ----------------------------------------------------

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "commands-service",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // We control when an event is considered complete.
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false
            };

            // ----------------------------------------------------
            // DLQ producer configuration
            // ----------------------------------------------------
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            using var dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();

            consumer.Subscribe(topic);

            _logger.LogInformation("Kafka consumer subscribed to {Topic}", topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    // ============================================
                    // CIRCUIT OPEN
                    // ============================================
                    if (_circuitState == CircuitState.Open)
                    {
                        HandleOpenCircuit(consumer, TimeSpan.FromSeconds(circuitOpenSeconds));
                        continue;
                    }

                    ConsumeResult<Ignore, string> result;

                    try
                    {
                        result = consumer.Consume(stoppingToken);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex,"Kafka consume error.");

                        await Task.Delay( 1000, stoppingToken);
                        continue;
                    }

                    _logger.LogInformation("Kafka message received. " + "Topic={Topic} Partition={Partition} Offset={Offset}",
                        result.Topic,
                        result.Partition,
                        result.Offset);

                    // ============================================
                    // PROCESS WITH RETRY
                    // ============================================
                    var processingResult = await ProcessWithRetryAsync(
                            result.Message.Value,
                            stoppingToken);

                    switch (processingResult.Status)
                    {
                        // ========================================
                        // SUCCESS
                        // ========================================
                        case ProcessingStatus.Success:
                        {
                            CommitOrSeek( consumer, result);

                            if (_circuitState == CircuitState.HalfOpen)
                            {
                                CloseCircuit();
                            }
                            break;
                        }

                        // ========================================
                        // PERMANENT / POISON MESSAGE
                        // ========================================

                        case ProcessingStatus.PermanentFailure:
                        {
                            _logger.LogError(processingResult.Exception,
                                "Permanent Kafka message failure. " +
                                "Sending event to DLQ.");

                            var dlqPublished =
                                await PublishToDlqAsync(
                                    dlqProducer,
                                    dlqTopic,
                                    result,
                                    processingResult.Exception!,
                                    processingResult.Attempts,
                                    stoppingToken);

                            if (dlqPublished)
                            {
                                // Important: Only commit the original record AFTER the DLQ write succeeds.
                                CommitOrSeek( consumer, result);
                            }
                            else
                            {
                                // DLQ itself failed.
                                // Rewind the consumer so this event will be seen again.

                                SeekSafely( consumer, result.TopicPartitionOffset);

                                _logger.LogWarning( "DLQ publish failed. " +
                                    "Original event will be retried.");

                                await Task.Delay( 2000,stoppingToken);
                            }

                            break;
                        }

                        // ========================================
                        // TRANSIENT FAILURE
                        // ========================================

                        case ProcessingStatus.TransientFailure:
                        {
                            _logger.LogError(processingResult.Exception,
                                "Transient dependency failure " +
                                "continued after retries. " +
                                "Opening circuit breaker.");

                            // VERY IMPORTANT:
                            // Consume() has already moved the consumer's local position forward.
                            // Since this event FAILED, rewind back to the failed offset.
                            SeekSafely(consumer,result.TopicPartitionOffset);
                            OpenCircuit(consumer);
                            break;
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consumer stopping.");
            }
            finally
            {
                consumer.Close();
            }
        }
    // ========================================================
    // RETRY + EXPONENTIAL BACKOFF
    // ========================================================
        private async Task<ProcessingResult>ProcessWithRetryAsync(string message, CancellationToken stoppingToken)
        {
            var maxAttempts = Math.Max(1,GetIntConfiguration("Kafka:Retry:MaxAttempts",4));

            var baseDelayMs = GetIntConfiguration("Kafka:Retry:BaseDelayMs", 500);

            var maxDelayMs = GetIntConfiguration("Kafka:Retry:MaxDelayMs", 5000);

            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    // Create a NEW scope for every attempt.
                    // This gives us a fresh DbContext if the previous database operation failed.
                    using var scope = _scopeFactory.CreateScope();

                    var eventProcessor = scope.ServiceProvider.GetRequiredService<IEventProcessor>();
                    eventProcessor.ProcessEvent(message);

                    return new ProcessingResult(ProcessingStatus.Success,  null,attempt);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                    when (IsTransient(ex))
                {
                    if (attempt == maxAttempts)
                    {
                        return new ProcessingResult(ProcessingStatus.TransientFailure,  ex, attempt);
                    }

                    // ----------------------------------------
                    // EXPONENTIAL BACKOFF
                    // ----------------------------------------
                    // attempt 1 -> 500ms
                    // attempt 2 -> 1000ms
                    // attempt 3 -> 2000ms
                    // plus jitter

                    var exponentialDelay = baseDelayMs * Math.Pow(2, attempt - 1);
                    var delayMs = Math.Min( maxDelayMs, exponentialDelay);

                    // Jitter prevents many consumers from retrying at exactly the same moment.

                    delayMs += Random.Shared.Next(0,250);

                    _logger.LogWarning( ex,
                        "Transient failure processing Kafka event. " +
                        "Attempt {Attempt}/{MaxAttempts}. " +
                        "Retrying in {DelayMs} ms.",
                        attempt,
                        maxAttempts,
                        delayMs);

                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), stoppingToken);
                }
                catch (Exception ex)
                {
                    // Anything we know is NOT transient becomes a permanent/poison failure.

                    return new ProcessingResult(ProcessingStatus.PermanentFailure, ex, attempt);
                }
            }

            throw new InvalidOperationException("Unexpected retry state.");
        }
        // ========================================================
        // TRANSIENT ERROR CLASSIFICATION
        // ========================================================
        private static bool IsTransient(Exception exception)
        {
            // Generic timeout
            if (exception is TimeoutException)
            {
                return true;
            }

            // EF Core often wraps SqlException inside DbUpdateException.
            if (exception is DbUpdateException dbUpdateException &&
                dbUpdateException.InnerException != null)
            {
                return IsTransient(dbUpdateException.InnerException);
            }

            if (exception is SqlException sqlException)
            {
                foreach (SqlError error in sqlException.Errors)
                {
                    if (IsTransientSqlError(error.Number))
                    {
                        return true;
                    }
                }
                return false;
            }

            // Sometimes networking/database errors are nested another level down.
            if (exception.InnerException != null)
            {
                return IsTransient( exception.InnerException);
            }

            return false;
        }

        private static bool IsTransientSqlError(int errorNumber)
        {
            return errorNumber switch
            {
                -2 => true,       // SQL command timeout
                53 => true,       // SQL Server unavailable
                64 => true,
                233 => true,
                1205 => true,     // deadlock
                10053 => true,
                10054 => true,
                10060 => true,
                // Azure SQL transient errors
                40197 => true,
                40501 => true,
                40613 => true,
                10928 => true,
                10929 => true,
                49918 => true,
                49919 => true,
                49920 => true,
                _ => false
            };
        }

        // ========================================================
        // CIRCUIT BREAKER
        // ========================================================
        private void OpenCircuit(IConsumer<Ignore, string> consumer)
        {
            _circuitState = CircuitState.Open;
            _circuitOpenedAtUtc = DateTimeOffset.UtcNow;

            var partitions = consumer.Assignment;

            if (partitions.Count > 0)
            {
                consumer.Pause(partitions);
            }

            _logger.LogWarning("Circuit breaker OPEN. " + "Paused {PartitionCount} Kafka partitions.", partitions.Count);
        }

        private void HandleOpenCircuit(IConsumer<Ignore, string> consumer, TimeSpan openDuration)
        {
            // Even though the partitions are paused, continue entering the Kafka consumer poll loop.
            // Do NOT simply Thread.Sleep(30 seconds).
            try
            {
                var unexpectedResult = consumer.Consume( TimeSpan.FromMilliseconds(250));

                // Normally nothing should be returned because the assigned partitions are paused.
                // This also protects us against assignment changes/rebalances.
                if (unexpectedResult != null)
                {
                    SeekSafely(consumer, unexpectedResult.TopicPartitionOffset);

                    consumer.Pause(
                        new[]
                        {
                            unexpectedResult.TopicPartition
                        });
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogWarning(ex, "Kafka polling error while circuit is open.");
            }

            var openFor =  DateTimeOffset.UtcNow - _circuitOpenedAtUtc;

            if (openFor < openDuration)
            {
                return;
            }

            // ================================================
            // OPEN -> HALF-OPEN
            // ================================================

            _circuitState = CircuitState.HalfOpen;

            var partitions = consumer.Assignment;

            if (partitions.Count > 0)
            {
                consumer.Resume(partitions);
            }

            _logger.LogInformation( "Circuit breaker HALF-OPEN. " +  "Allowing one processing attempt.");
        }

        private void CloseCircuit()
        {
            _circuitState = CircuitState.Closed;
            _logger.LogInformation("Circuit breaker CLOSED. " +  "Dependency is healthy again.");
        }

        // ========================================================
        // DLQ
        // ========================================================
        private async Task<bool> PublishToDlqAsync(
            IProducer<string, string> producer,
            string dlqTopic,
            ConsumeResult<Ignore, string> originalMessage,
            Exception exception,
            int processingAttempts,
            CancellationToken stoppingToken)
        {
            const int maxDlqAttempts = 3;

            var messageId =
                $"{originalMessage.Topic}-" +
                $"{originalMessage.Partition.Value}-" +
                $"{originalMessage.Offset.Value}";

            var dlqPayload =
                JsonSerializer.Serialize(
                    new
                    {
                        id = messageId,

                        originalTopic =  originalMessage.Topic,

                        originalPartition = originalMessage.Partition.Value,

                        originalOffset =  originalMessage.Offset.Value,

                        payload = originalMessage.Message.Value,

                        processingAttempts,

                        errorType =  exception.GetType().FullName,

                        errorMessage =  exception.Message,

                        failedAtUtc =   DateTimeOffset.UtcNow,

                        consumerGroup = "commands-service"
                    });


            for (var attempt = 1; attempt <= maxDlqAttempts;  attempt++)
            {
                try
                {
                    var deliveryResult =
                        await producer.ProduceAsync(dlqTopic,
                            new Message<string, string>
                            {
                                // Deterministic identifier based
                                // on original Kafka location.
                                Key = messageId,
                                Value = dlqPayload
                            },
                            stoppingToken);

                    _logger.LogWarning(
                        "Event published to DLQ {DlqTopic}. " +
                        "DLQOffset={Offset}. " +
                        "Original={OriginalLocation}",
                        dlqTopic,
                        deliveryResult.Offset,
                        messageId);

                    return true;
                }
                catch (ProduceException<string, string> ex)
                {
                    _logger.LogError( ex,
                        "Failed publishing to DLQ. " +
                        "Attempt {Attempt}/{MaxAttempts}.",
                        attempt,
                        maxDlqAttempts);

                    if (attempt < maxDlqAttempts)
                    {
                        var delay = TimeSpan.FromMilliseconds(500 * Math.Pow( 2, attempt - 1));

                        await Task.Delay(  delay, stoppingToken);
                    }
                }
            }
            return false;
        }

        // ========================================================
        // OFFSET MANAGEMENT
        // ========================================================
        private void CommitOrSeek(IConsumer<Ignore, string> consumer, ConsumeResult<Ignore, string> result)
        {
            try
            {
                consumer.Commit(result);

                _logger.LogInformation("Kafka offset committed. " +
                    "Partition={Partition} Offset={Offset}",
                    result.Partition,
                    result.Offset);
            }
            catch (KafkaException ex)
            {
                // Business operation succeeded,
                // but Kafka commit failed.
                //
                // Reprocessing is possible.
                //
                // Your EventProcessor should therefore
                // remain idempotent.

                _logger.LogError( ex,
                    "Kafka commit failed. " +
                    "Event may be processed again.");

                SeekSafely(consumer,result.TopicPartitionOffset);
            }
        }


        private void SeekSafely(IConsumer<Ignore, string> consumer, TopicPartitionOffset offset)
        {
            try
            {
                consumer.Seek(offset);

                _logger.LogInformation(
                    "Kafka consumer rewound to " +
                    "{TopicPartitionOffset}",
                    offset);
            }
            catch (KafkaException ex)
            {
                _logger.LogError( ex,
                    "Could not seek Kafka consumer to " +
                    "{TopicPartitionOffset}",
                    offset);
            }
        }

        // ========================================================
        // CONFIG HELPERS
        // ========================================================

        private int GetIntConfiguration(string key, int defaultValue)
        {
            return int.TryParse(_configuration[key],out var value)
                    ? value
                    : defaultValue;
        }

        // ========================================================
        // TYPES
        // ========================================================

        private enum CircuitState
        {
            Closed,
            Open,
            HalfOpen
        }

        private enum ProcessingStatus
        {
            Success,
            TransientFailure,
            PermanentFailure
        }

        private sealed record ProcessingResult(
            ProcessingStatus Status,
            Exception? Exception,
            int Attempts);
    }
}
