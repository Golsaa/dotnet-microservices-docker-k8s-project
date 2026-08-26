namespace PlatformService.Dtos
{
    public class PlatformPublishedDto
    {
        public Guid EventId { get; set; }
        public int Id { get; set; }  //PlatformId
        public string Name { get; set; }
        public string Event { get; set; }

        public DateTime OccurredAtUtc { get; set; }
        public int SchemaVersion { get; set; } = 1;
    }
}