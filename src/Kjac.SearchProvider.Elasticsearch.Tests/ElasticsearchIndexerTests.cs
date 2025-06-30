using Elastic.Clients.Elasticsearch;
using Kjac.SearchProvider.Elasticsearch.Services;
using ExistsResponse = Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

public class ElasticsearchIndexerTests : ElasticsearchTestBase
{
    [Test]
    public async Task CanCreateAndResetIndex()
    {
        IElasticsearchIndexManager indexManager = GetRequiredService<IElasticsearchIndexManager>();
        IElasticsearchIndexer indexer = GetRequiredService<IElasticsearchIndexer>();
        ElasticsearchClient client = GetRequiredService<IElasticsearchClientFactory>().GetClient();

        const string indexAlias = "someindex";

        await indexManager.EnsureAsync(indexAlias);

        ExistsResponse existsResponse = await client.Indices.ExistsAsync(indexAlias);
        Assert.That(existsResponse.Exists, Is.True);

        await indexer.ResetAsync(indexAlias);

        existsResponse = await client.Indices.ExistsAsync(indexAlias);
        Assert.That(existsResponse.Exists, Is.False);
    }
}
