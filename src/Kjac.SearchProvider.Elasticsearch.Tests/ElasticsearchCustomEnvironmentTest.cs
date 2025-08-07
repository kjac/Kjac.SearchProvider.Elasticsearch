using Elastic.Clients.Elasticsearch;
using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.Services;
using Microsoft.Extensions.DependencyInjection;
using ExistsResponse = Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

public class ElasticsearchCustomEnvironmentTest : ElasticsearchTestBase
{
    private const string IndexAlias = "someindex";
    private const string Environment = "test";

    protected override void PerformAdditionalConfiguration(ServiceCollection serviceCollection)
        => serviceCollection.Configure<ClientOptions>(
            options =>
            {
                options.Environment = Environment;
            }
        );

    protected override async Task PerformOneTimeSetUpAsync()
        => await DeleteIndex(EnvironmentIndexAlias());

    protected override async Task PerformOneTimeTearDownAsync()
        => await DeleteIndex(EnvironmentIndexAlias());

    [Test]
    public async Task CanCreateCustomEnvironmentIndex()
    {
        ElasticsearchClient client = GetRequiredService<IElasticsearchClientFactory>().GetClient();

        await IndexManager.EnsureAsync(IndexAlias);

        ExistsResponse existsResponse = await client.Indices.ExistsAsync(EnvironmentIndexAlias());
        Assert.That(existsResponse.Exists, Is.True);
    }

    private IElasticsearchIndexManager IndexManager => GetRequiredService<IElasticsearchIndexManager>();

    private string EnvironmentIndexAlias() => $"{IndexAlias}_{Environment}";
}
