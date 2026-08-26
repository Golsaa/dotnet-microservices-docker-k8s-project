using AutoMapper;
using PlatformService.Dtos;
using PlatformService.Data;
using Microsoft.AspNetCore.Mvc;
using PlatformService.Models;
using PlatformService.SyncDataServices.Http;
using System.Threading.Tasks;
using PlatformService.AsyncDataServices;

namespace PlatformService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PlatformsController : ControllerBase
    {
        private readonly IPlatformRepo _repository;
        private readonly ICommandDataClient _commandDataClient;
        private readonly IMapper _mapper;
        private readonly  IMessageBusClient _messageBusClient; 

        public PlatformsController(
            IPlatformRepo repository, 
            ICommandDataClient commandDataClient,
            IMapper mapper,
            IMessageBusClient messageBusClient)
            {
            _repository = repository;
            _commandDataClient = commandDataClient;
            _mapper = mapper;
            _messageBusClient = messageBusClient;
        }

        [HttpGet]
        public ActionResult<IEnumerable<PlatformReadDto>> GetPlatforms()
        {
            Console.WriteLine(" -- > Getting Platforms .... ");
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

            //Send Sync Message
            /*try
            {
               await _commandDataClient.SendPlatformToCommand(platformReadDto);
            }
            catch(Exception ex)
            {
                Console.WriteLine( $"--> Could not send synchronously: {ex.Message}");
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
                Console.WriteLine( $"--> Could not send asynchronously!: {ex.Message}");
            }

            return CreatedAtRoute("GetPlatformById", new {id = platformReadDto.Id} , platformReadDto); 
        }
    }
 }