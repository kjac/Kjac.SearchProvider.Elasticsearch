using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.Services;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Models.Indexing;
using Umbraco.Cms.Search.Core.Models.Searching;
using Umbraco.Cms.Search.Core.Models.Searching.Filtering;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

// tests for phrase / next-word autocomplete suggestions (default IndexerOptions:
// UseSuggestions = true, suggestions sourced from the __allTextsR1 field)
public class ElasticsearchSuggestionTests : ElasticsearchTestBase
{
    private const string IndexAlias = "suggestindex";
    private const string FieldTitle = "title";
    private const string FieldTag = "tag";

    private readonly Guid _principalId = Guid.NewGuid();

    protected override void PerformAdditionalConfiguration(ServiceCollection serviceCollection)
        => serviceCollection.Configure<IndexerOptions>(options => options.UseSuggestions = true);

    protected override async Task PerformOneTimeSetUpAsync()
    {
        await EnsureIndex();

        // core / filter scoping data (invariant, public)
        await AddDocumentAsync("red boat", tag: "a");
        await AddDocumentAsync("red board", tag: "b");
        await AddDocumentAsync("red book", tag: "b");
        await AddDocumentAsync("read the news", tag: "b");
        await AddDocumentAsync("reading list", tag: "b");

        // access scoping data (invariant, protected)
        await AddDocumentAsync("secret manuscript", protection: new ContentProtection([_principalId]));

        // culture scoping data (single document varying by culture)
        await AddCultureDocumentAsync("umbraco workflow", "umbrella stand");

        await WaitForIndexingOperationsToCompleteAsync();
    }

    protected override async Task PerformOneTimeTearDownAsync()
        => await DeleteIndex(IndexAlias);

    [Test]
    public async Task SuggestsNextWordFromSingleTypedWord()
    {
        SearchResult result = await SearchAsync(query: "re", maxSuggestions: 20);

        Assert.That(result.Suggestions, Is.Not.Null);
        Assert.That(result.Suggestions, Does.Contain("red"));
        Assert.That(result.Suggestions, Does.Contain("read"));
    }

    [Test]
    public async Task CompletesTrailingWordPreservingTypedPrefix()
    {
        SearchResult result = await SearchAsync(query: "red bo", maxSuggestions: 20);

        Assert.That(result.Suggestions, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result.Suggestions, Does.Contain("red boat"));
            Assert.That(result.Suggestions, Does.Contain("red board"));
            Assert.That(result.Suggestions!.All(s => s.StartsWith("red bo")), Is.True);
        });
    }

    [Test]
    public async Task LimitsSuggestionsToMaxSuggestions()
    {
        SearchResult result = await SearchAsync(query: "red", maxSuggestions: 2);

        Assert.That(result.Suggestions, Is.Not.Null);
        Assert.That(result.Suggestions!.Count(), Is.LessThanOrEqualTo(2));
    }

    [Test]
    public async Task ProducesNoSuggestionsWhenMaxSuggestionsIsZero()
    {
        SearchResult result = await SearchAsync(query: "red", maxSuggestions: 0);

        Assert.That(result.Suggestions ?? [], Is.Empty);
    }

    [Test]
    public async Task ProducesNoSuggestionsForEmptyQuery()
    {
        SearchResult result = await SearchAsync(query: " ", maxSuggestions: 20);

        Assert.That(result.Suggestions ?? [], Is.Empty);
    }

    [Test]
    public async Task ProducesNoSuggestionsWhenNothingMatches()
    {
        SearchResult result = await SearchAsync(query: "zzz", maxSuggestions: 20);

        Assert.That(result.Suggestions ?? [], Is.Empty);
    }

    [Test]
    public async Task RespectsRegularFilter()
    {
        SearchResult result = await SearchAsync(
            query: "red bo",
            maxSuggestions: 20,
            filters: [new KeywordFilter(FieldTag, ["a"], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Suggestions, Does.Contain("red boat"));
            Assert.That(result.Suggestions, Does.Not.Contain("red board"));
            Assert.That(result.Suggestions, Does.Not.Contain("red book"));
        });
    }

    [Test]
    public async Task RespectsNegatedFilter()
    {
        SearchResult result = await SearchAsync(
            query: "red bo",
            maxSuggestions: 20,
            filters: [new KeywordFilter(FieldTag, ["a"], true)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Suggestions, Does.Not.Contain("red boat"));
            Assert.That(result.Suggestions, Does.Contain("red board"));
        });
    }

    [Test]
    public async Task ExcludesProtectedSuggestionsForAnonymous()
    {
        SearchResult result = await SearchAsync(query: "secr", maxSuggestions: 20);

        Assert.That(result.Suggestions ?? [], Is.Empty);
    }

    [Test]
    public async Task IncludesProtectedSuggestionsForPrincipal()
    {
        SearchResult result = await SearchAsync(
            query: "secr",
            maxSuggestions: 20,
            accessContext: new AccessContext(_principalId, null)
        );

        Assert.That(result.Suggestions, Does.Contain("secret"));
    }

    [Test]
    public async Task ScopesSuggestionsByCulture()
    {
        SearchResult english = await SearchAsync(query: "umbr", maxSuggestions: 20, culture: "en-US");
        SearchResult danish = await SearchAsync(query: "umbr", maxSuggestions: 20, culture: "da-DK");

        Assert.Multiple(() =>
        {
            Assert.That(english.Suggestions, Does.Contain("umbraco"));
            Assert.That(english.Suggestions, Does.Not.Contain("umbrella"));

            Assert.That(danish.Suggestions, Does.Contain("umbrella"));
            Assert.That(danish.Suggestions, Does.Not.Contain("umbraco"));
        });
    }

    private async Task<SearchResult> SearchAsync(
        string? query = null,
        IEnumerable<Filter>? filters = null,
        string? culture = null,
        AccessContext? accessContext = null,
        int maxSuggestions = 0)
    {
        IElasticsearchSearcher searcher = GetRequiredService<IElasticsearchSearcher>();
        SearchResult result = await searcher.SearchAsync(
            IndexAlias,
            query: query,
            filters: filters,
            culture: culture,
            accessContext: accessContext,
            take: 100,
            maxSuggestions: maxSuggestions
        );

        Assert.That(result, Is.Not.Null);
        return result;
    }

    private async Task AddDocumentAsync(string title, string? tag = null, ContentProtection? protection = null)
    {
        IElasticsearchIndexer indexer = GetRequiredService<IElasticsearchIndexer>();
        var fields = new List<IndexField>
        {
            new(FieldTitle, new IndexValue { TextsR1 = [title] }, Culture: null, Segment: null)
        };
        if (tag is not null)
        {
            fields.Add(new IndexField(FieldTag, new IndexValue { Keywords = [tag] }, Culture: null, Segment: null));
        }

        await indexer.AddOrUpdateAsync(
            IndexAlias,
            Guid.NewGuid(),
            UmbracoObjectTypes.Document,
            [new Variation(Culture: null, Segment: null)],
            fields,
            protection
        );
    }

    private async Task AddCultureDocumentAsync(string englishTitle, string danishTitle)
    {
        IElasticsearchIndexer indexer = GetRequiredService<IElasticsearchIndexer>();
        await indexer.AddOrUpdateAsync(
            IndexAlias,
            Guid.NewGuid(),
            UmbracoObjectTypes.Document,
            [
                new Variation(Culture: "en-US", Segment: null),
                new Variation(Culture: "da-DK", Segment: null)
            ],
            [
                new IndexField(FieldTitle, new IndexValue { TextsR1 = [englishTitle] }, Culture: "en-US", Segment: null),
                new IndexField(FieldTitle, new IndexValue { TextsR1 = [danishTitle] }, Culture: "da-DK", Segment: null)
            ],
            null
        );
    }

    private async Task EnsureIndex()
    {
        await DeleteIndex(IndexAlias);
        await GetRequiredService<IElasticsearchIndexManager>().EnsureAsync(IndexAlias);
    }
}
