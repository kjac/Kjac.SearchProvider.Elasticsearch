namespace Kjac.SearchProvider.Elasticsearch.Configuration;

public record BasicAuthentication
{
    public required string Username { get; init; }

    public required string Password { get; init; }
}