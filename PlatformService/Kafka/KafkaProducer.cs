using Confluent.Kafka;
using System.Text.Json;

public class KafkaProducer
{
    // Defines a high-level Apache Kafka producer client that provides key and value serialization.
    private readonly IProducer<string, string> _producer;

    public KafkaProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",

            // Strongest acknowledgement mode
            Acks = Acks.All,

            // Prevent duplicates caused by producer retries
            EnableIdempotence = true,

            // Maximum time the producer will keep trying
            MessageTimeoutMs = 30000
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishOrderCreatedAsync(OrderCreatedEvent order)
    {
        var message = new Message<string, string>
        {
            // Same OrderId always goes to the same partition
            Key = order.OrderId,
            Value = JsonSerializer.Serialize(order)
        };

        try
        {
            var result = await _producer.ProduceAsync("order-created", message);

            Console.WriteLine(
                $"Message successfully published. " +
                $"Topic: {result.Topic}, " +
                $"Partition: {result.Partition}, " +
                $"Offset: {result.Offset}");
        }
        catch (ProduceException<string, string> ex)
        {
            Console.WriteLine($"Kafka publish failed: {ex.Error.Reason}");

            throw;
        }
    }
}