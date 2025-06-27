namespace Kjac.SearchProvider.Elasticsearch.Configuration;

internal sealed class ClientOptions
{
    public Uri? Host { get; set; }
    
    public BasicAuthenticationOptions? BasicAuthentication { get; set; }

    public bool EnableDebugMode { get; set; }
}