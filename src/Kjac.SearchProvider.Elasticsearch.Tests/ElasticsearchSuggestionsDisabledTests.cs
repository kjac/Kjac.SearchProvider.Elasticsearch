using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.Services;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Models.Indexing;
using Umbraco.Cms.Search.Core.Models.Searching;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

// verifies that IndexerOptions.UseSuggestions = false skips suggestion indexing and querying entirely
public class ElasticsearchSuggestionsDisabledTests : ElasticsearchTestBase
{
    private const string IndexAlias = "suggestdisabledindex";

    protected override void PerformAdditionalConfiguration(ServiceCollection serviceCollection)
        => serviceCollection.Configure<IndexerOptions>(options => options.UseSuggestions = false);

    protected override async Task PerformOneTimeSetUpAsync()
    {
        await EnsureIndex();

        IElasticsearchIndexer indexer = GetRequiredService<IElasticsearchIndexer>();
        await indexer.AddOrUpdateAsync(
            IndexAlias,
            Guid.NewGuid(),
            UmbracoObjectTypes.Document,
            [new Variation(Culture: null, Segment: null)],
            [
                new IndexField("title", new IndexValue { TextsR1 = ["gamma ray"] }, Culture: null, Segment: null)
            ],
            null
        );

        await WaitForIndexingOperationsToCompleteAsync();
    }

    protected override async Task PerformOneTimeTearDownAsync()
        => await DeleteIndex(IndexAlias);

    [Test]
    public async Task ProducesNoSuggestionsWhenDisabled()
    {
        IElasticsearchSearcher searcher = GetRequiredService<IElasticsearchSearcher>();
        SearchResult result = await searcher.SearchAsync(IndexAlias, query: "gam", take: 100, maxSuggestions: 20);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Suggestions ?? [], Is.Empty);
    }

    private async Task EnsureIndex()
    {
        await DeleteIndex(IndexAlias);
        await GetRequiredService<IElasticsearchIndexManager>().EnsureAsync(IndexAlias);
    }
}
