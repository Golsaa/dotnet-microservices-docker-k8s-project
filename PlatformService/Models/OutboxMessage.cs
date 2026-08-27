namespace PlatformService.Models;

public class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();

    public string Type { get; set; } = null!;

    public string Payload { get; set; } = null!;

    public string CorrelationId { get; set; } = null!;

    public DateTime OccurredOnUtc { get; set; } = DateTime.UtcNow;

    public DateTime? ProcessedOnUtc { get; set; }

    public int RetryCount { get; set; }

    public string? LastError { get; set; }
}