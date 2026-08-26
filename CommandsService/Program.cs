using CommandsService.Data;
using Microsoft.EntityFrameworkCore;
using CommandsService.EventProcessing;
using CommandsService.AsyncDataServices;

using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Logs;

var builder = WebApplication.CreateBuilder(args);

var serviceName = "CommandsService";

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource
        .AddService(serviceName: serviceName))
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        //.AddProcessInstrumentation()
        .AddMeter(serviceName)
        .AddOtlpExporter())
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddSource(serviceName)
        .AddOtlpExporter());

////In Below Logging.AddOpenTelemetry "IncludeScopes = true" is especially important for  the Kafka consumer because it exports scoped values such as CorrelationId,KafkaTopic, KafkaPartition, KafkaOffset
builder.Logging.AddOpenTelemetry(logging =>
{
   logging.IncludeFormattedMessage = true;
   logging.IncludeScopes = true;
   logging.ParseStateValues = true;
   logging.AddOtlpExporter();
});

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddControllers();
builder.Services.AddAutoMapper(cfg => { }, AppDomain.CurrentDomain.GetAssemblies());
builder.Services.AddScoped<IEventProcessor, EventProcessor>();
builder.Services.AddScoped<ICommandRepo, CommandRepo>();
builder.Services.AddHostedService<KafkaConsumerService>();

builder.Services.AddDbContext<AppDbContext>(options => options.UseSqlServer(builder.Configuration.GetConnectionString("CommandsConn")));



var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    dbContext.Database.Migrate();
}

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

///This one is not needed in this project and can be removed,  but I don't know what is it for at all
app.UseHttpsRedirection();

app.MapControllers();
app.Run();

