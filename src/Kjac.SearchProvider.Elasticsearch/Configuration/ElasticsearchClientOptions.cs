namespace Kjac.SearchProvider.Elasticsearch.Configuration;

internal sealed class ElasticsearchClientOptions
{
    public Uri? Host { get; set; }
    
    public Authentication? Authentication { get; set; }

    public bool EnableDebugMode { get; set; }

    public int MaxFacetValues { get; set; }
}