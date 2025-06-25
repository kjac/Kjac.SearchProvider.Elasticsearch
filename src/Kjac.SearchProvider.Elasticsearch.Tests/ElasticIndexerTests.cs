using Kjac.SearchProvider.Elasticsearch.Services;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

public class ElasticIndexerTests : ElasticTestBase
{
    [Test]
    public async Task CanCreateAndResetIndex()
    {
        var indexer = GetRequiredService<IElasticIndexer>();
        var client = GetRequiredService<IElasticClientFactory>().GetClient();

        const string indexAlias = "someindex";

        await indexer.EnsureAsync(indexAlias);

        var existsResponse = await client.Indices.ExistsAsync(indexAlias);
        Assert.That(existsResponse.Exists, Is.True);

        await indexer.ResetAsync(indexAlias);

        existsResponse = await client.Indices.ExistsAsync(indexAlias);
        Assert.That(existsResponse.Exists, Is.False);
    }
}