using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.DependencyInjection;
using Kjac.SearchProvider.Elasticsearch.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.Sync;
using ExistsResponse = Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

[TestFixture]
public abstract class ElasticsearchTestBase
{
    private ServiceProvider _serviceProvider;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        IConfigurationRoot configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        var serviceCollection = new ServiceCollection();
        serviceCollection
            .AddElasticsearch(configuration)
            .AddLogging();

        serviceCollection.Configure<SearcherOptions>(
            options =>
            {
                options.MaxFacetValues = 500;
            }
        );

        serviceCollection.AddSingleton<IServerRoleAccessor, SingleServerRoleAccessor>();

        PerformAdditionalConfiguration(serviceCollection);

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

    protected virtual void PerformAdditionalConfiguration(ServiceCollection serviceCollection)
    {
    }

    protected virtual Task PerformOneTimeSetUpAsync()
        => Task.CompletedTask;

    protected virtual Task PerformOneTimeTearDownAsync()
        => Task.CompletedTask;

    protected T GetRequiredService<T>() where T : notnull
        => _serviceProvider.GetRequiredService<T>();

    protected Task WaitForIndexingOperationsToCompleteAsync()
    {
        // https://www.elastic.co/docs/reference/elasticsearch/rest-apis/refresh-parameter
        // "Elasticsearch automatically refreshes shards that have changed every index.refresh_interval which defaults to one second"
        Thread.Sleep(1000);
        return Task.CompletedTask;
    }

    protected async Task DeleteIndex(string indexAlias)
    {
        ElasticsearchClient client = GetRequiredService<IElasticsearchClientFactory>().GetClient();

        ExistsResponse existsResponse = await client.Indices.ExistsAsync(indexAlias);
        if (existsResponse.Exists)
        {
            DeleteIndexResponse response = await client.Indices.DeleteAsync(indexAlias);
            Assert.That(response.IsValidResponse, Is.True);
        }
    }
}
