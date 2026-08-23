using System.Text.Json;
using AutoMapper;
using CommandsService.Data;
using CommandsService.Dtos;
using CommandsService.Models;

namespace CommandsService.EventProcessing
{
    public class EventProcessor : IEventProcessor
    {
        private readonly ICommandRepo _repository;
        private readonly IMapper _mapper;
        private readonly ILogger<EventProcessor> _logger;

        private readonly JsonSerializerOptions _jsonOptions = new()
            {
                PropertyNameCaseInsensitive = true
            };

        public EventProcessor(ICommandRepo repository, IMapper mapper, ILogger<EventProcessor> logger)
        {
            _repository = repository;
            _mapper = mapper;
            _logger = logger;
        }

        public void ProcessEvent(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
            {
                throw new ArgumentException("Kafka message cannot be empty.", nameof(message));
            }

            var eventType = DetermineEventType(message);

            switch (eventType)
            {
                case EventType.PlatformPublished:

                    AddPlatform(message);
                    break;

                case EventType.Undetermined:

                    _logger.LogWarning("Could not determine Kafka event type. Message: {Message}",message);
                    break;

                default:

                    _logger.LogWarning("Unhandled Kafka event type: {EventType}",eventType);
                    break;
            }
        }


        private EventType DetermineEventType(string notificationMessage)
        {
            try
            {
                var eventDto = JsonSerializer.Deserialize<GenericEventDto>(notificationMessage, _jsonOptions);

                if (eventDto == null)
                {
                    return EventType.Undetermined;
                }

                return eventDto.Event switch
                {
                    "Platform_Published" => EventType.PlatformPublished,
                    _ => EventType.Undetermined
                };
            }
            catch (JsonException ex)
            {
                _logger.LogError( ex, "Unable to deserialize Kafka event.");
                throw;
            }
        }

        private void AddPlatform(string platformPublishedMessage)
        {
            var platformPublishedDto = JsonSerializer.Deserialize<PlatformPublishedDto>(platformPublishedMessage, _jsonOptions);

            if (platformPublishedDto == null)
            {
                throw new InvalidOperationException("Platform_Published event could not be deserialized.");
            }

            var platformModel = _mapper.Map<Platform>(platformPublishedDto);

            // Idempotency check
            if (_repository.ExternalPlatformExists(
                    platformModel.ExternalId))
            {
                _logger.LogInformation("Platform {ExternalPlatformId} already exists. Skipping duplicate Kafka event.",
                    platformModel.ExternalId);

                return;
            }

            _repository.CreatePlatform(platformModel);
            _repository.SaveChanges();
            _logger.LogInformation("Platform {ExternalPlatformId} added to Commands Service.", platformModel.ExternalId);
        }
    }

}
