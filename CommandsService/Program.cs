using CommandsService.Data;
using Microsoft.EntityFrameworkCore;
using CommandsService.EventProcessing;
using CommandsService.AsyncDataServices;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddControllers();
builder.Services.AddAutoMapper( cfg => { }, AppDomain.CurrentDomain.GetAssemblies());
builder.Services.AddScoped<IEventProcessor,  EventProcessor>();
builder.Services.AddScoped<ICommandRepo, CommandRepo>();
builder.Services.AddHostedService<KafkaConsumerService>();
    
builder.Services.AddDbContext<AppDbContext>(options => options.UseSqlServer(builder.Configuration.GetConnectionString("PlatformsConn")));
/*else{
    Console.WriteLine(" --> Using InMem Db");
    builder.Services.AddDbContext<AppDbContext>(options =>
        options.UseInMemoryDatabase("InMem"));}*/



var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

///This one is not needed in this project and can be removed,  but I don't know what is it for at all
app.UseHttpsRedirection();

app.MapControllers();
app.Run();

