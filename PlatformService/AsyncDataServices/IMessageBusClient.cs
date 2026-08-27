using PlatformService.Dtos;

namespace PlatformService.AsyncDataServices
{
    public interface  IMessageBusClient
    {
        Task PublishNewPlatformAsync(PlatformPublishedDto platformPublishedDto, string correlationId, string eventId, CancellationToken cancellationToken = default);
    }
}