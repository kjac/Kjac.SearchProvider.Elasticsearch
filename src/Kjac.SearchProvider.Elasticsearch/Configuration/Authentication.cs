namespace Kjac.SearchProvider.Elasticsearch.Configuration;

public record Authentication
{
    public BasicAuthentication? Basic { get; init; }
}