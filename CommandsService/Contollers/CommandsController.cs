using AutoMapper;
using CommandsService.Data;
using CommandsService.Dtos;
using CommandsService.Models;
using Microsoft.AspNetCore.Mvc;

namespace CommandsService.Controllers
{
    [Route("api/cs/platforms/{platformId}/[controller]")]
    [ApiController]
    public class CommandsController : ControllerBase
    {
        private readonly IMapper _mapper;
        private readonly ICommandRepo _commandRepository;

        public CommandsController(ICommandRepo repository, IMapper mapper){
            _commandRepository = repository;
            _mapper = mapper;
        }

        [HttpGet]
        public ActionResult<IEnumerable<CommandReadDto>> GetCommandsForPlatform(int platformId)
        {
            Console.WriteLine($"--> Hit GetCommandsForPlatform for platformId {platformId}");
            if (!_commandRepository.PlatformExits(platformId))
            {
                return NotFound();
            }
            var commands = _commandRepository.GetCommandsForPlatform(platformId);
            return Ok(_mapper.Map<IEnumerable<CommandReadDto>>(commands));

        }

        // Ex: http://localhost:5117/api/cs/platforms/1/commands/3
        [HttpGet("{commandId}", Name = "GetCommandForPlatform")]
        public ActionResult<CommandReadDto> GetCommand(int platformId, int commandId)
        {
            Console.WriteLine($"--> Hit GetCommand platformId/commandId: {platformId}/{commandId}");
            if (!_commandRepository.PlatformExits(platformId))
            {
                return NotFound();
            }
            var command = _commandRepository.GetCommand(platformId,commandId);
            if(command == null)
            {
                 return NotFound();
            }

            return Ok(_mapper.Map<CommandReadDto>(command));
        }

        [HttpPost]
        public ActionResult<CommandReadDto> CreateCommandForPlatform(int platformId, CommandCreateDto commandCreateDto)
        {
            Console.WriteLine($"--> Hit CreateCommandForPlatform platformId: {platformId}");
            if (!_commandRepository.PlatformExits(platformId))
            {
                return NotFound();
            } 

            var command = _mapper.Map<Command>(commandCreateDto);
            _commandRepository.CreateCommand(platformId, command);
            _commandRepository.SaveChanges();

            var commandReadDto = _mapper.Map<CommandReadDto>(command);

            return CreatedAtRoute(nameof(GetCommandsForPlatform), 
            new { platformId = platformId, commandId = commandReadDto.Id }, commandReadDto );
                

        }
    }
}