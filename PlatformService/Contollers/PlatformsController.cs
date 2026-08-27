using AutoMapper;
using PlatformService.Dtos;
using PlatformService.Data;
using Microsoft.AspNetCore.Mvc;
using PlatformService.Models;
using PlatformService.SyncDataServices.Http;
using PlatformService.AsyncDataServices;
using Asp.Versioning;

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
        private readonly IPlatformRepo _repository;
        private readonly ICommandDataClient _commandDataClient;
        private readonly IMapper _mapper;
        private readonly  IMessageBusClient _messageBusClient; 
        private readonly ILogger<PlatformsController> _logger;

        public PlatformsController(
            IPlatformRepo repository, 
            ICommandDataClient commandDataClient,
            IMapper mapper,
            IMessageBusClient messageBusClient,
             ILogger<PlatformsController> logger)
            {
            _repository = repository;
            _commandDataClient = commandDataClient;
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
        public async Task<ActionResult<PlatformReadDto>> CreatePlatform(PlatformCreateDto platformCreateDto)
        {
            var platformModel = _mapper.Map<Platform>(platformCreateDto);
             _repository.CreatePlatform(platformModel);
             _repository.SaveChanges();

            var platformReadDto = _mapper.Map<PlatformReadDto>(platformModel);

            _logger.LogInformation("Platform created. PlatformId={PlatformId}, Name={PlatformName}",
                platformReadDto.Id, platformReadDto.Name);

            //Send Sync Message
            /*try
            {
               await _commandDataClient.SendPlatformToCommand(platformReadDto);
            }
            catch(Exception ex)
            {
               _logger.LogDebug( $"--> Could not send synchronously: {ex.Message}");
            }*/

            //Send Async MEssage
            try
            {
               var platformPublishedDto = _mapper.Map<PlatformPublishedDto>(platformReadDto);
               platformPublishedDto.Event = "Platform_Published";

               var correlationId = HttpContext.TraceIdentifier;

               await _messageBusClient.PublishNewPlatformAsync(platformPublishedDto, correlationId);
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