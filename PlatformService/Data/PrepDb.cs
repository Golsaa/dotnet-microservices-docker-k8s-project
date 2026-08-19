using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using PlatformService.Models;

namespace PlatformService.Data
{
public static class PrepDb
{
   public static void PrepPopulation(IApplicationBuilder app, IWebHostEnvironment env){
        using( var serviceScope = app.ApplicationServices.CreateScope())
            {
               //Another option to get envo var
               //var env = serviceScope.ServiceProvider.GetRequiredService<IWebHostEnvironment>();

               if (env.IsProduction())
                {
                    Console.WriteLine("--> Attempting to apply migrations ...");
                    try
                    {
                        
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine($"--> Could not run migrations: {ex.Message}");
                    }

                }
                //GetRequiredService VS GetService:
                //GetService<AppDbContext>() can return null, while GetRequiredService<AppDbContext>() 
                //immediately tells you if you've forgotten to register the context. 
                SeedData(serviceScope.ServiceProvider.GetRequiredService<AppDbContext>());
            }
        }

        private static void SeedData(AppDbContext context)
        {   if(true)
            {
                context.Database.Migrate();
            }
            else
            {
                
            }
            if(!context.Platforms.Any())
            {
                Console.WriteLine(" -- > Seeding Data ... ");

                context.Platforms.AddRange(
                new Platform() {Name="Dot Net", Publisher="Microsoft", Cost="Free"},
                new Platform() {Name="SQL Server Express", Publisher="Microsoft", Cost="Free"},
                new Platform() {Name="Kubernetes", Publisher="Cloud Native Computing Foundation", Cost="Free"});

                context.SaveChanges();
            }
            else
            {
            Console.WriteLine(" -- > We already have data");
            }
        }
}
}