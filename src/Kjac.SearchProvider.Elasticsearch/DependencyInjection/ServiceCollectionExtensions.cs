using Kjac.SearchProvider.Elasticsearch.Services;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Search.Core.Services;

namespace Kjac.SearchProvider.Elasticsearch.DependencyInjection;

internal static class ServiceCollectionExtensions
{
    public static IServiceCollection AddElastic(this IServiceCollection services)
    {
        // register the Elastic searcher and indexer so they can be used explicitly for index registrations
        services.AddTransient<IElasticIndexer, ElasticIndexer>();
        services.AddTransient<IElasticSearcher, ElasticSearcher>();

        // register the Elastic searcher and indexer as the defaults
        services.AddTransient<IIndexer, ElasticIndexer>();
        services.AddTransient<ISearcher, ElasticSearcher>();

        services.AddSingleton<ElasticClientFactory>();

        return services;
    }
}