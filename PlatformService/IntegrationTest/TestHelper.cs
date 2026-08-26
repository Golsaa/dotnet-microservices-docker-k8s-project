namespace PlatformService.IntegrationTest
{
    public static class TestHelper
    {
        private static async Task<T> WaitUntilAsync<T>(Func<Task<T?>> check, TimeSpan timeout, TimeSpan pollInterval) where T : class
        {
            var deadline = DateTime.UtcNow + timeout;

            while (DateTime.UtcNow < deadline)
            {
                var result = await check();

                if (result is not null)
                {
                    return result;
                }

                await Task.Delay(pollInterval);
            }

            throw new TimeoutException($"Expected result was not available within {timeout}.");
        }
    }
    ///Use it in a Kafka integration test:
    /// SAMPLE: 
    /*await producer.ProduceAsync(topic, kafkaMessage);

    var savedPlatform = await WaitUntilAsync(
        () => repository.GetByExternalIdAsync(externalId),
        timeout: TimeSpan.FromSeconds(15),
        pollInterval: TimeSpan.FromMilliseconds(250));

    Assert.Equal("Docker", savedPlatform.Name); */
}