public class OrderCreatedEvent
{
    public string OrderId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}
