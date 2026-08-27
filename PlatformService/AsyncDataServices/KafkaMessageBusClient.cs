using Confluent.Kafka;
using PlatformService.Dtos;
using System.Text.Json;
using System.Text;
using Microservices.Contracts.Kafka;

namespace PlatformService.AsyncDataServices
{
    public class KafkaMessageBusClient : IMessageBusClient, IDisposable
    {
        private readonly IProducer<string, string> _producer;
        private readonly string _topic;
        private readonly ILogger<KafkaMessageBusClient> _logger;

        public KafkaMessageBusClient(IConfiguration configuration, ILogger<KafkaMessageBusClient> logger)
        {
            var bootstrapServers = configuration["Kafka:BootstrapServers"] ??
                                     throw new InvalidOperationException("Kafka BootstrapServers is not configured.");

            _topic = configuration["Kafka:Topic"] ?? "platform-published";
            _logger = logger;

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,

                // Wait for Kafka's strongest normal acknowledgement
                Acks = Acks.All,

                // Protect against duplicates caused by producer retries
                EnableIdempotence = true,

                ClientId = "platform-service"
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task PublishNewPlatformAsync(PlatformPublishedDto platformPublishedDto, string correlationId, string eventId,
                                                    CancellationToken cancellationToken = default)
        {
            platformPublishedDto.Event = "Platform_Published";
            var eventType = "PlatformPublished.v1";

            var messageValue = JsonSerializer.Serialize(platformPublishedDto);
            var headers = new Headers
                {
                    { "correlation-id", Encoding.UTF8.GetBytes(correlationId) },
                    { "event-id", Encoding.UTF8.GetBytes(eventId) },
                    { "event-type", Encoding.UTF8.GetBytes(eventType) }
                };


            var message = new Message<string, string>
            {
                Key = platformPublishedDto.Id.ToString(),
                Value = messageValue,
                Headers = headers
            };

            try
            {
                var result = await _producer.ProduceAsync(_topic, message, cancellationToken);

                _logger.LogInformation( "Published Kafka outbox event. EventId={EventId}, PlatformId={PlatformId}, Topic={Topic}, Partition={Partition}, Offset={Offset}",
                    eventId,
                    platformPublishedDto.Id,
                    result.Topic,
                    result.Partition.Value,
                    result.Offset.Value);
            }
            ////ProduceException<string, string>  Represents an error that occured whilst producing a message.
            catch (ProduceException<string, string> ex)
            {
               _logger.LogError(ex,"Kafka publish failed. Topic={Topic}, PlatformId={PlatformId}",
                    _topic, platformPublishedDto.Id);

                    throw;
            }
        }

        public void Dispose()
        {
            // Gives queued messages an opportunity to finish
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
    }
}
