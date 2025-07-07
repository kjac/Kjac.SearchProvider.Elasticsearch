using Elastic.Clients.Elasticsearch;
using Kjac.SearchProvider.Elasticsearch.Services;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Extensions;
using Umbraco.Cms.Search.Core.Models.Indexing;
using Umbraco.Cms.Search.Core.Models.Searching;
using Umbraco.Cms.Search.Core.Models.Searching.Faceting;
using Umbraco.Cms.Search.Core.Models.Searching.Filtering;
using Umbraco.Cms.Search.Core.Models.Searching.Sorting;
using ExistsResponse = Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

public class ElasticsearchIndexerTests : ElasticsearchTestBase
{
    private const string IndexAlias = "someindex";

    protected override async Task PerformOneTimeSetUpAsync()
        => await Indexer.ResetAsync(IndexAlias);

    protected override async Task PerformOneTimeTearDownAsync()
        => await Indexer.ResetAsync(IndexAlias);

    [Test]
    public async Task CanCreateAndResetIndex()
    {
        ElasticsearchClient client = GetRequiredService<IElasticsearchClientFactory>().GetClient();

        await IndexManager.EnsureAsync(IndexAlias);

        ExistsResponse existsResponse = await client.Indices.ExistsAsync(IndexAlias);
        Assert.That(existsResponse.Exists, Is.True);

        await Indexer.ResetAsync(IndexAlias);

        existsResponse = await client.Indices.ExistsAsync(IndexAlias);
        Assert.That(existsResponse.Exists, Is.False);
    }

    [Test]
    public async Task CanDeleteRootDocuments()
    {
        await IndexManager.EnsureAsync(IndexAlias);

        Dictionary<string, Guid> ids = await CreateIndexStructure();

        await Indexer.DeleteAsync(IndexAlias, [ids["0:root"], ids["2:root"]]);

        await WaitForIndexingOperationsToCompleteAsync();

        for (var i = 0; i < 3; i++)
        {
            SearchResult result = await SearchAsync(
                filters: [
                    new KeywordFilter(
                        Umbraco.Cms.Search.Core.Constants.FieldNames.PathIds,
                        [ids[$"{i}:root"].AsKeyword()],
                        false
                    )
                ]
            );

            Assert.That(result.Total, Is.EqualTo(i == 1 ? 3 : 0));
        }

        await Indexer.ResetAsync(IndexAlias);
    }

    [Test]
    public async Task CanDeleteDescendantDocuments()
    {
        await IndexManager.EnsureAsync(IndexAlias);

        Dictionary<string, Guid> ids = await CreateIndexStructure();

        await Indexer.DeleteAsync(IndexAlias, [ids["1:child"], ids["2:grandchild"]]);

        await WaitForIndexingOperationsToCompleteAsync();

        for (var i = 0; i < 3; i++)
        {
            SearchResult result = await SearchAsync(
                filters: [
                    new KeywordFilter(
                        Umbraco.Cms.Search.Core.Constants.FieldNames.PathIds,
                        [ids[$"{i}:root"].AsKeyword()],
                        false
                    )
                ]
            );

            Assert.That(
                result.Total,
                Is.EqualTo(
                    i switch
                    {
                        0 => 3, // all documents should still be there
                        1 => 1, // child and grandchild should be deleted
                        2 => 2  // grandchild should be deleted
                    }
                )
            );
        }

        await Indexer.ResetAsync(IndexAlias);
    }

    private async Task<Dictionary<string, Guid>> CreateIndexStructure()
    {
        var ids = new Dictionary<string, Guid>();
        for (var i = 0; i < 3; i++)
        {
            var rootId = Guid.NewGuid();
            var childId = Guid.NewGuid();
            var grandchildId = Guid.NewGuid();
            ids.Add($"{i}:root", rootId);
            ids.Add($"{i}:child", childId);
            ids.Add($"{i}:grandchild", grandchildId);

            await Indexer.AddOrUpdateAsync(
                IndexAlias,
                rootId,
                UmbracoObjectTypes.Unknown,
                [new Variation(Culture: null, Segment: null)],
                [
                    new IndexField(
                        Umbraco.Cms.Search.Core.Constants.FieldNames.PathIds,
                        new IndexValue { Keywords = [rootId.AsKeyword()] },
                        Culture: null,
                        Segment: null
                    )
                ],
                null
            );

            await Indexer.AddOrUpdateAsync(
                IndexAlias,
                childId,
                UmbracoObjectTypes.Unknown,
                [new Variation(Culture: null, Segment: null)],
                [
                    new IndexField(
                        Umbraco.Cms.Search.Core.Constants.FieldNames.PathIds,
                        new IndexValue { Keywords = [rootId.AsKeyword(), childId.AsKeyword()] },
                        Culture: null,
                        Segment: null
                    )
                ],
                null
            );

            await Indexer.AddOrUpdateAsync(
                IndexAlias,
                grandchildId,
                UmbracoObjectTypes.Unknown,
                [new Variation(Culture: null, Segment: null)],
                [
                    new IndexField(
                        Umbraco.Cms.Search.Core.Constants.FieldNames.PathIds,
                        new IndexValue { Keywords = [rootId.AsKeyword(), childId.AsKeyword(), grandchildId.AsKeyword()] },
                        Culture: null,
                        Segment: null
                    )
                ],
                null
            );
        }

        await WaitForIndexingOperationsToCompleteAsync();

        for (var i = 0; i < 3; i++)
        {
            SearchResult result = await SearchAsync(
                filters: [
                    new KeywordFilter(
                        Umbraco.Cms.Search.Core.Constants.FieldNames.PathIds,
                        [ids[$"{i}:root"].AsKeyword()],
                        false
                    )
                ]
            );

            Assert.That(result.Total, Is.EqualTo(3));
        }

        return ids;
    }

    private IElasticsearchIndexManager IndexManager => GetRequiredService<IElasticsearchIndexManager>();

    private IElasticsearchIndexer Indexer => GetRequiredService<IElasticsearchIndexer>();

    private IElasticsearchSearcher Searcher => GetRequiredService<IElasticsearchSearcher>();

    private async Task<SearchResult> SearchAsync(
        string? query = null,
        IEnumerable<Filter>? filters = null,
        IEnumerable<Facet>? facets = null,
        IEnumerable<Sorter>? sorters = null,
        string? culture = null,
        string? segment = null,
        AccessContext? accessContext = null,
        int skip = 0,
        int take = 100)
    {
        SearchResult result = await Searcher.SearchAsync(
            IndexAlias,
            query,
            filters,
            facets,
            sorters,
            culture,
            segment,
            accessContext,
            skip,
            take
        );

        Assert.That(result, Is.Not.Null);
        return result;
    }
}
