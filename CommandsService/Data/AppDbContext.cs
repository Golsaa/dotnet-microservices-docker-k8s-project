using Microsoft.EntityFrameworkCore;
using CommandsService.Models;

namespace CommandsService.Data
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> opt): base(opt)
        {
            
        }

        public DbSet<Platform> Platforms { get; set; }
        public DbSet<Command> Commands { get; set; }

        //You override it when you want to explicitly tell EF Core how your database model should be structured, 
        // including relationships, keys, constraints, table names, etc.
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            //A Platform has many Commands, and each Command has one Platform.
           modelBuilder
            .Entity<Platform>()
            .HasMany(p => p.Commands)
            .WithOne(c => c.Platform)
            .HasForeignKey(c => c.PlatformId);

            //Technically we don't need both, normally, configure it only once:
            //A Command has one Platform, and each Platform has many Commands.
            modelBuilder
                .Entity<Command>()
                .HasOne(p => p.Platform)
                .WithMany(p => p.Commands)
                .HasForeignKey(p => p.PlatformId);
        }
    }
}