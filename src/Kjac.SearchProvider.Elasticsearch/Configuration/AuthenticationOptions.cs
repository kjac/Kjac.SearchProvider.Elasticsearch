namespace Kjac.SearchProvider.Elasticsearch.Configuration;

public record AuthenticationOptions
{
    public string? Username { get; init; }

    public string? Password { get; init; }

    public string? ApiKey { get; init; }
}