using Elastic.Clients.Elasticsearch;

namespace Kjac.SearchProvider.Elasticsearch.Services;

public interface IElasticClientFactory
{
    ElasticsearchClient GetClient();
}