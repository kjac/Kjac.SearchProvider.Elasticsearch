namespace Kjac.SearchProvider.Elasticsearch.Configuration;

public sealed class IndexerOptions
{
    public bool UseSuggestions { get; set; }

    public string[] SuggestionFields { get; set; } = [];
}
