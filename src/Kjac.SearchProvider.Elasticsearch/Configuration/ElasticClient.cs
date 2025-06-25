namespace Kjac.SearchProvider.Elasticsearch.Configuration;

public class ElasticClient
{
    // TODO: configure host options here
    
    public Authentication? Authentication { get; set; }

    public bool EnableDebugMode { get; set; }

    public int MaxFacetValues { get; set; }
}