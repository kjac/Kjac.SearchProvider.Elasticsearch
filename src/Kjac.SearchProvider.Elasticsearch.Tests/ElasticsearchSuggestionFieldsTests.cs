using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.Services;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Models.Indexing;
using Umbraco.Cms.Search.Core.Models.Searching;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

// verifies that IndexerOptions.SuggestionFields restricts the suggestion source to the configured fields
public class ElasticsearchSuggestionFieldsTests : ElasticsearchTestBase
{
    private const string IndexAlias = "suggestfieldsindex";
    private const string FieldTitle = "title";
    private const string FieldBody = "body";

    protected override void PerformAdditionalConfiguration(ServiceCollection serviceCollection)
        => serviceCollection.Configure<IndexerOptions>(options =>
            {
                options.UseSuggestions = true;
                options.SuggestionFields = [FieldTitle];
            }
        );

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
                new IndexField(FieldTitle, new IndexValue { TextsR1 = ["alpha title"] }, Culture: null, Segment: null),
                new IndexField(FieldBody, new IndexValue { TextsR1 = ["beta content"] }, Culture: null, Segment: null)
            ],
            null
        );

        await WaitForIndexingOperationsToCompleteAsync();
    }

    protected override async Task PerformOneTimeTearDownAsync()
        => await DeleteIndex(IndexAlias);

    [Test]
    public async Task SuggestsFromConfiguredField()
    {
        SearchResult result = await SearchAsync("alph");

        Assert.That(result.Suggestions, Does.Contain("alpha"));
    }

    [Test]
    public async Task DoesNotSuggestFromUnconfiguredField()
    {
        SearchResult result = await SearchAsync("bet");

        Assert.That(result.Suggestions ?? [], Is.Empty);
    }

    private async Task<SearchResult> SearchAsync(string query)
    {
        IElasticsearchSearcher searcher = GetRequiredService<IElasticsearchSearcher>();
        SearchResult result = await searcher.SearchAsync(IndexAlias, query: query, take: 100, maxSuggestions: 20);

        Assert.That(result, Is.Not.Null);
        return result;
    }

    private async Task EnsureIndex()
    {
        await DeleteIndex(IndexAlias);
        await GetRequiredService<IElasticsearchIndexManager>().EnsureAsync(IndexAlias);
    }
}
