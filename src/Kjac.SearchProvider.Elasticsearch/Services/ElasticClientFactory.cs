using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Options;
using Kjac.SearchProvider.Elasticsearch.Configuration;
using BasicAuthentication = Elastic.Transport.BasicAuthentication;

namespace Kjac.SearchProvider.Elasticsearch.Services;

internal sealed class ElasticClientFactory
{
    private readonly ElasticsearchClient _client;

    public ElasticClientFactory(IOptions<ElasticClient> options)
    {
        var elasticClientOptions = options.Value;
        var settings = new ElasticsearchClientSettings();
        if (elasticClientOptions.Authentication?.Basic is not null)
        {
            settings.Authentication(
                new BasicAuthentication(
                    elasticClientOptions.Authentication.Basic.Username,
                    elasticClientOptions.Authentication.Basic.Password
                )
            );
        }

        if (elasticClientOptions.EnableDebugMode)
        {
            settings.EnableDebugMode();
        }

        _client = new ElasticsearchClient(settings);
    }

    public ElasticsearchClient GetClient() => _client;
}