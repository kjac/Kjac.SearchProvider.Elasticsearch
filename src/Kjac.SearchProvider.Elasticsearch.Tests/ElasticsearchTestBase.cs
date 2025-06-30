using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.Sync;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

[TestFixture]
public abstract class ElasticsearchTestBase
{
    private ServiceProvider _serviceProvider;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        var serviceCollection = new ServiceCollection();
        serviceCollection
            .AddElasticsearch(configuration)
            .AddLogging();

        serviceCollection.Configure<SearcherOptions>(options =>
        {
            options.MaxFacetValues = 500;
        });

        serviceCollection.AddSingleton<IServerRoleAccessor, SingleServerRoleAccessor>();

        _serviceProvider = serviceCollection.BuildServiceProvider();

        await PerformOneTimeSetUpAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await PerformOneTimeTearDownAsync();

        if (_serviceProvider is IDisposable disposableServiceProvider)
        {
            disposableServiceProvider.Dispose();
        }
    }

    protected virtual Task PerformOneTimeSetUpAsync()
        => Task.CompletedTask;

    protected virtual Task PerformOneTimeTearDownAsync()
        => Task.CompletedTask;

    protected T GetRequiredService<T>() where T : notnull
        => _serviceProvider.GetRequiredService<T>();
}