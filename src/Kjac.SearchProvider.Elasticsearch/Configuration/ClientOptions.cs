namespace Kjac.SearchProvider.Elasticsearch.Configuration;

internal sealed class ClientOptions
{
    public Uri? Host { get; set; }
    
    public AuthenticationOptions? Authentication { get; set; }

    public bool EnableDebugMode { get; set; }
}