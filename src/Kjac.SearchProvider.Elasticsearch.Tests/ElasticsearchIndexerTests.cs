using Kjac.SearchProvider.Elasticsearch.Services;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

public class ElasticsearchIndexerTests : ElasticsearchTestBase
{
    [Test]
    public async Task CanCreateAndResetIndex()
    {
        var indexManager = GetRequiredService<IElasticsearchIndexManager>();
        var indexer = GetRequiredService<IElasticsearchIndexer>();
        var client = GetRequiredService<IElasticsearchClientFactory>().GetClient();

        const string indexAlias = "someindex";

        await indexManager.EnsureAsync(indexAlias);

        var existsResponse = await client.Indices.ExistsAsync(indexAlias);
        Assert.That(existsResponse.Exists, Is.True);

        await indexer.ResetAsync(indexAlias);

        existsResponse = await client.Indices.ExistsAsync(indexAlias);
        Assert.That(existsResponse.Exists, Is.False);
    }
}