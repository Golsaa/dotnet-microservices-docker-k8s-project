using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using PlatformService.Data;
using PlatformService.Dtos;

namespace PlatformService.AsyncDataServices;

public class OutboxProcessor : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OutboxProcessor> _logger;

    public OutboxProcessor(
        IServiceScopeFactory scopeFactory,
        ILogger<OutboxProcessor> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessPendingMessagesAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while processing outbox messages.");
            }

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }

    private async Task ProcessPendingMessagesAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();

        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var messageBusClient = scope.ServiceProvider.GetRequiredService<IMessageBusClient>();

        var messages = await context.OutboxMessages
            .Where(x => x.ProcessedOnUtc == null)
            .OrderBy(x => x.OccurredOnUtc)
            .Take(20)
            .ToListAsync(cancellationToken);

        foreach (var message in messages)
        {
            try
            {
                var platformEvent = JsonSerializer.Deserialize<PlatformPublishedDto>(message.Payload)
                    ?? throw new InvalidOperationException(
                        $"Outbox event {message.Id} has an invalid payload.");

                await messageBusClient.PublishNewPlatformAsync(
                    platformEvent,
                    message.CorrelationId,
                    message.Id.ToString("N"),
                    cancellationToken);

                message.ProcessedOnUtc = DateTime.UtcNow;
                message.LastError = null;

                await context.SaveChangesAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                message.RetryCount++;
                message.LastError = ex.Message;

                await context.SaveChangesAsync(cancellationToken);

                _logger.LogError(ex,
                    "Outbox publish failed. EventId={EventId}, EventType={EventType}, RetryCount={RetryCount}",
                    message.Id,
                    message.Type,
                    message.RetryCount);
            }
        }
    }
}