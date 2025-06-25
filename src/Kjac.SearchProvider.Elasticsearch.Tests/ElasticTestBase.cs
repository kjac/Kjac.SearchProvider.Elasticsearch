using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.Sync;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

[TestFixture]
public abstract class ElasticTestBase
{
    protected ServiceProvider ServiceProvider { get; private set; }

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var serviceCollection = new ServiceCollection();
        serviceCollection
            .AddElastic()
            .AddLogging();

        serviceCollection.Configure<ElasticClient>(options =>
        {
            options.Authentication = new()
            {
                Basic = new()
                {
                    Username = "elastic",
                    Password = "o7WGEZFC"
                }
            };

            options.MaxFacetValues = 500;
        });

        serviceCollection.AddSingleton<IServerRoleAccessor, SingleServerRoleAccessor>();

        ServiceProvider = serviceCollection.BuildServiceProvider();

        await PerformOneTimeSetUpAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await PerformOneTimeTearDownAsync();

        if (ServiceProvider is IDisposable disposableServiceProvider)
        {
            disposableServiceProvider.Dispose();
        }
    }

    protected virtual Task PerformOneTimeSetUpAsync()
        => Task.CompletedTask;

    protected virtual Task PerformOneTimeTearDownAsync()
        => Task.CompletedTask;

    protected T GetRequiredService<T>() where T : notnull
        => ServiceProvider.GetRequiredService<T>();
}