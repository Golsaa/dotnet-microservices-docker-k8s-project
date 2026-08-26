/*CHECK THE DEPENDENCIES ARE THERE 
dotnet add package Testcontainers.MsSql --version 4.14.0
dotnet add package Microsoft.EntityFrameworkCore.SqlServer --version 9.0.9
dotnet add package Microsoft.EntityFrameworkCore.Design --version 9.0.9
dotnet add package xunit.v3 --version 4.0.0
dotnet add package xunit.runner.visualstudio --version 4.0.0


using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;
using Xunit;

public sealed class PlatformRepositoryIntegrationTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlServer = new MsSqlBuilder()
        .WithPassword("Your_strong_Password123!")
        .Build();

    private AppDbContext _dbContext = null!;

    public async Task InitializeAsync()
    {
        await _sqlServer.StartAsync();

        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlServer(_sqlServer.GetConnectionString())
            .Options;

        _dbContext = new AppDbContext(options);

        // Important: tests your real EF Core migrations.
        await _dbContext.Database.MigrateAsync();
    }

    [Fact]
    public async Task CreatePlatform_PersistsPlatformInRealSqlServer()
    {
        // Arrange
        var platform = new Platform
        {
            Name = "Docker",
            Publisher = "Docker Inc.",
            Cost = "Free"
        };

        // Act
        _dbContext.Platforms.Add(platform);
        await _dbContext.SaveChangesAsync();

        // Assert
        var savedPlatform = await _dbContext.Platforms
            .SingleAsync(x => x.Id == platform.Id);

        Assert.Equal("Docker", savedPlatform.Name);
        Assert.Equal("Docker Inc.", savedPlatform.Publisher);
    }

    public async Task DisposeAsync()
    {
        await _dbContext.DisposeAsync();
        await _sqlServer.DisposeAsync();
    }
}
*/