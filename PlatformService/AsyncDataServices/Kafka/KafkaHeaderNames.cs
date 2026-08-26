namespace Microservices.Contracts.Kafka;

/// <summary>
/// "The Kafka header used for correlation IDs is called correlation-id"
/// </summary>
public static class KafkaHeaderNames
{
    public const string CorrelationId = "correlation-id";
    public const string EventId = "event-id";
    public const string EventType = "event-type";
}