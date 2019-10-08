using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans;
using Orleans.Concurrency;
using Orleans.Configuration;
using Orleans.Hosting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    public interface IIndexGrain : IGrainWithStringKey
    {
        Task<string> GetId(string name);
    }

    public interface IWorkerGrain : IGrainWithStringKey
    {
        Task DoWork();
    }

    public sealed class IndexGrain : Grain, IIndexGrain
    {
        public Task<string> GetId(string name)
        {
            return Task.FromResult($"{name}_worker");
        }
    }

    public sealed class WorkerGrain : Grain, IWorkerGrain
    {
        public async Task DoWork()
        {
            await Task.Delay(50);
        }
    }

    public sealed class Checker : IHostedService
    {
        private readonly IGrainFactory grainFactory;
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private Task task;

        public Checker(IGrainFactory grainFactory)
        {
            this.grainFactory = grainFactory;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            task = Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    await Task.Delay(1000);

                    var watch = Stopwatch.StartNew();

                    var index = grainFactory.GetGrain<IIndexGrain>("Default");

                    await index.GetId("1");

                    watch.Stop();

                    Console.WriteLine("Elapsed: {0}", watch.Elapsed);
                }
            });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            cts.Cancel();

            return task;
        }
    }

    public sealed class Worker : IHostedService
    {
        private readonly IGrainFactory grainFactory;
        private readonly List<Task> tasks = new List<Task>();
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private int counter;

        public Worker(IGrainFactory grainFactory)
        {
            this.grainFactory = grainFactory;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            tasks.Add(Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    await Task.Delay(1000);

                    Console.WriteLine("Handled: {0}", counter);

                    counter = 0;
                }
            }));

            for (var i = 0; i < 100; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    await Run(i + 1);
                }));
            }

            return Task.CompletedTask;
        }

        private async Task Run(int i)
        {
            Console.WriteLine("Started worker {0}", i);

            while (!cts.Token.IsCancellationRequested)
            {
                var index = grainFactory.GetGrain<IIndexGrain>("Default");

                var workerId = await index.GetId(i.ToString());
                var worker = grainFactory.GetGrain<IWorkerGrain>(workerId);

                await worker.DoWork();

                Interlocked.Increment(ref counter);
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            cts.Cancel();

            return Task.WhenAll(tasks);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Host.CreateDefaultBuilder(args)
                .UseOrleans(builder =>
                {
                    var siloPort = 11111;
                    var siloAddress = IPAddress.Loopback;

                    int gatewayPort = 30000;

                    builder.UseDevelopmentClustering(options => options.PrimarySiloEndpoint = new IPEndPoint(siloAddress, siloPort));
                    builder.UseInMemoryReminderService();
                    builder.ConfigureEndpoints(siloAddress, siloPort, gatewayPort);
                    builder.Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = "helloworldcluster";
                        options.ServiceId = "1";
                    });

                    builder.ConfigureApplicationParts(appParts => appParts.AddApplicationPart(typeof(Program).Assembly));
                })
                .ConfigureServices(services =>
                {
                    services.AddHostedService<Worker>();
                    services.AddHostedService<Checker>();
                })
                .Build()
                .Run();
        }
    }
}
