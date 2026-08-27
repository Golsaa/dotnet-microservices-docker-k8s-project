using AutoMapper;
using PlatformService.Dtos;
using PlatformService.Data;
using Microsoft.AspNetCore.Mvc;
using PlatformService.Models;
using PlatformService.SyncDataServices.Http;
using PlatformService.AsyncDataServices;
using Asp.Versioning;
using System.Text.Json;

namespace PlatformService.Controllers
{
    //ASP.NET Core routes are case-insensitive, but the gateway’s path matching can be case-sensitive.
    // So using [Route("api/[controller]")], the request may never reach PlatformsController cause it generates sth like "	
    //http://microservices.local:8888/api/Platforms/", notice the capital P. So I'm making the controller route explicit and lowercase:
    //[Route("api/[controller]")]

    //For when I'm adding versioning,
    // Also add to program.cs: builder.Services.AddApiVersioning(opt..).AddApiExplorer(opt..)
    //If adding versioning, the Gateway HTTPRoute must also change its platform prefix from /api/platforms to /api/v1/platforms (or simply match /api).
    //[Route("api/v{version:apiVersion}/platforms")]

    [ApiVersion(1)]
    [Route("api/v{version:apiVersion}/platforms")]          
    [ApiController]
    public class PlatformsController : ControllerBase
    {
        private readonly AppDbContext _context;
        private readonly IPlatformRepo _repository;
        private readonly IMapper _mapper;
        private readonly  IMessageBusClient _messageBusClient; 
        private readonly ILogger<PlatformsController> _logger;

        public PlatformsController(
            AppDbContext context,
            IPlatformRepo repository, 
            IMapper mapper,
            IMessageBusClient messageBusClient,
            ILogger<PlatformsController> logger)
            {
            _context = context;
            _repository = repository;
            _mapper = mapper;
            _messageBusClient = messageBusClient;
            _logger = logger;
        }

        [HttpGet]
        public ActionResult<IEnumerable<PlatformReadDto>> GetPlatforms()
        {
          _logger.LogDebug("Retrieving all platforms.");
          var platformItems = _repository.GetAllPlatforms();

          return Ok(_mapper.Map<IEnumerable<PlatformReadDto>>(platformItems)); 
        }

        [HttpGet("{id}", Name = "GetPlatformById")]
        public ActionResult<PlatformReadDto> GetPlatformById(int id)
        {
            var platformItem = _repository.GetPlatformById(id);
            if (platformItem != null)
            {
                return Ok(_mapper.Map<PlatformReadDto>(platformItem));
            }
            return NotFound();
        }


        [HttpPost]
        public async Task<ActionResult<PlatformReadDto>> CreatePlatform( PlatformCreateDto platformCreateDto,
            CancellationToken cancellationToken)
        {
            var platform = _mapper.Map<Platform>(platformCreateDto);

            _context.Platforms.Add(platform);

            var publishedEvent = _mapper.Map<PlatformPublishedDto>(platform);
            publishedEvent.Event = "Platform_Published";

            var outboxMessage = new OutboxMessage
            {
                Type = "PlatformPublished.v1",
                Payload = JsonSerializer.Serialize(publishedEvent),
                CorrelationId = HttpContext.TraceIdentifier
            };

            _context.OutboxMessages.Add(outboxMessage);

            // EF Core sends both inserts to SQL Server as one unit of work. 
            // If the database save fails, neither the platform nor the outbox event should be committed. 
            // If Kafka is temporarily unavailable later, the outbox row stays pending and the processor retries it.
            await _context.SaveChangesAsync(cancellationToken);

            var platformReadDto = _mapper.Map<PlatformReadDto>(platform);

            _logger.LogInformation(
                "Platform and outbox event saved. PlatformId={PlatformId}, EventId={EventId}, CorrelationId={CorrelationId}",
                platform.Id,
                outboxMessage.Id,
                outboxMessage.CorrelationId);

            return CreatedAtRoute(
                "GetPlatformById",
                new { id = platformReadDto.Id },
                platformReadDto);
        }

        [HttpPost("experiments/direct-kafka")]
        public async Task<ActionResult<PlatformReadDto>> CreatePlatformNoOutboxSupport(PlatformCreateDto platformCreateDto)
        {
            var platformModel = _mapper.Map<Platform>(platformCreateDto);
             _repository.CreatePlatform(platformModel);
             _repository.SaveChanges();

            var platformReadDto = _mapper.Map<PlatformReadDto>(platformModel);

            _logger.LogInformation("Platform created. PlatformId={PlatformId}, Name={PlatformName}",
                platformReadDto.Id, platformReadDto.Name);

        
            //Send Async MEssage
            try
            {
               var platformPublishedDto = _mapper.Map<PlatformPublishedDto>(platformReadDto);
               platformPublishedDto.Event = "Platform_Published";

               var correlationId = HttpContext.TraceIdentifier;

               await _messageBusClient.PublishNewPlatformAsync(platformPublishedDto, correlationId, Guid.NewGuid().ToString("N"));
            }
            catch(Exception ex)
            {
              _logger.LogError( ex, "Platform was saved but its Kafka event could not be published. PlatformId={PlatformId}",
                     platformReadDto.Id);
            }

            return CreatedAtRoute("GetPlatformById", new {id = platformReadDto.Id} , platformReadDto); 
        }
    }
 }