using Microsoft.EntityFrameworkCore;
using PlatformService.Models;

namespace PlatformService.Data
{
    public static class PrepDb
    {
        public static void PrepPopulation(IApplicationBuilder app, IWebHostEnvironment env)
        {
            //Another option to get envo var
            //var env = serviceScope.ServiceProvider.GetRequiredService<IWebHostEnvironment>();
            //if (env.IsProduction())

            using var serviceScope = app.ApplicationServices.CreateScope();

            var context = serviceScope.ServiceProvider
                .GetRequiredService<AppDbContext>();

            context.Database.Migrate();

            if (!context.Platforms.Any())
            {
                Console.WriteLine("--> Seeding data...");

                context.Platforms.AddRange(
                    new Platform { Name = "Dot Net", Publisher = "Microsoft", Cost = "Free" },
                    new Platform
                    {
                        Name = "Kubernetes",
                        Publisher = "Cloud Native Computing Foundation",
                        Cost = "Free"
                    });

                context.SaveChanges();
            }
            else
            {
                Console.WriteLine("--> Platform data already exists.");
            }
        }
    }
}