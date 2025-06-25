namespace Kjac.SearchProvider.Elasticsearch.Configuration;

public class ElasticClient
{
    public Uri? Host { get; set; }
    
    public Authentication? Authentication { get; set; }

    public bool EnableDebugMode { get; set; }

    public int MaxFacetValues { get; set; }
}