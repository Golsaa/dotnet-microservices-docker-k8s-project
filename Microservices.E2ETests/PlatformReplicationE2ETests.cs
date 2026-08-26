using System.Net;
using System.Net.Http.Json;
using Xunit;

namespace Microservices.E2ETests;

public class PlatformReplicationE2ETests
{
    private readonly HttpClient _client;

    public PlatformReplicationE2ETests()
    {
        var baseUrl = Environment.GetEnvironmentVariable("E2E_BASE_URL")?? "http://microservices.local:8888";

        _client = new HttpClient
        {
            BaseAddress = new Uri(baseUrl),
            Timeout = TimeSpan.FromSeconds(30)
        };
    }

    [Fact]
    public async Task Creating_platform_eventually_replicates_it_to_commands_service()
    {
        // Unique name prevents collisions with manual data and parallel test runs.
        var uniqueName = $"E2E Docker {Guid.NewGuid():N}";

        var request = new CreatePlatformRequest(
            Name: uniqueName,
            Publisher: "Docker Inc.",
            Cost: "Free");

        // 1. Start the real workflow through the public gateway route.
        var createResponse = await _client.PostAsJsonAsync("/api/platforms", request);

        createResponse.EnsureSuccessStatusCode();

        var createdPlatform = await createResponse.Content.ReadFromJsonAsync<PlatformResponse>();

        Assert.NotNull(createdPlatform);

        // 2. Kafka processing is asynchronous.
        // Poll Commands Service until its local copy becomes available.
        var replicatedPlatform = await WaitUntilAsync(
            async () =>
            {
                var response = await _client.GetAsync(
                    $"/api/cs/platforms/{createdPlatform.Id}");

                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }

                response.EnsureSuccessStatusCode();

                return await response.Content.ReadFromJsonAsync<CommandPlatformResponse>();
            },
            timeout: TimeSpan.FromSeconds(20),
            pollInterval: TimeSpan.FromMilliseconds(250));

        // 3. Verify the client-visible business result.
        Assert.Equal(createdPlatform.Id, replicatedPlatform.ExternalId);
        Assert.Equal(uniqueName, replicatedPlatform.Name);
        Assert.Equal("Docker Inc.", replicatedPlatform.Publisher);
    }

    private static async Task<T> WaitUntilAsync<T>(
        Func<Task<T?>> check,
        TimeSpan timeout,
        TimeSpan pollInterval)
        where T : class
    {
        var deadline = DateTime.UtcNow + timeout;
        Exception? lastException = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var result = await check();

                if (result is not null)
                {
                    return result;
                }
            }
            catch (HttpRequestException exception)
            {
                // Allows for a temporary service/gateway connection failure.
                lastException = exception;
            }

            await Task.Delay(pollInterval);
        }

        throw new TimeoutException(
            $"The platform was not replicated within {timeout}.",
            lastException);
    }

    private sealed record CreatePlatformRequest(
        string Name,
        string Publisher,
        string Cost);

    private sealed record PlatformResponse(
        int Id,
        string Name,
        string Publisher,
        string Cost);

    private sealed record CommandPlatformResponse(
        int Id,
        int ExternalId,
        string Name,
        string Publisher);
}
