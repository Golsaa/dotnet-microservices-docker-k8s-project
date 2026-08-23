using Confluent.Kafka;
using PlatformService.Dtos;
using System.Text.Json;

namespace PlatformService.AsyncDataServices
{
    public class KafkaMessageBusClient : IMessageBusClient, IDisposable
    {
        private readonly IProducer<string, string> _producer;
        private readonly string _topic;

        public KafkaMessageBusClient(IConfiguration configuration)
        {
            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? 
                                     throw new InvalidOperationException("Kafka BootstrapServers is not configured.");

            _topic = configuration["Kafka:Topic"] ?? "platform-published";

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

        public async Task PublishNewPlatformAsync(PlatformPublishedDto platformPublishedDto)
        {
            platformPublishedDto.Event = "Platform_Published";

            var message = JsonSerializer.Serialize(platformPublishedDto);

            try
            {
                var result = await _producer.ProduceAsync(
                    _topic,
                    new Message<string, string>
                    {
                        // Same platform goes to same partition
                        Key = platformPublishedDto.Id.ToString(),
                        Value = message
                    });

                Console.WriteLine($"--> Kafka message published " +
                    $"Topic: {result.Topic}, " +
                    $"Partition: {result.Partition}, " +
                    $"Offset: {result.Offset}");
            }
            ////ProduceException<string, string>  Represents an error that occured whilst producing a message.
            catch (ProduceException<string, string> ex)
            {
                Console.WriteLine($"--> Kafka publish failed: {ex.Error.Reason}");
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
