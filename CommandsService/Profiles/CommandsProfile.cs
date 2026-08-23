using AutoMapper;
using CommandsService.Dtos;
using CommandsService.Models;

namespace CommandsService.Profiles
{
    public class CommandsProfile : Profile
    {
        public CommandsProfile()
        {
            // Source -> Target
           CreateMap<Platform, PlatformReadDto>();
           CreateMap<CommandCreateDto, Command>();
           CreateMap<Command, CommandReadDto>();

           // Kafka event DTO -> Commands Service domain model
           CreateMap<PlatformPublishedDto, Platform>().ForMember(
                    dest => dest.ExternalId,
                    opt => opt.MapFrom(src => src.Id));
        }
    }
}

