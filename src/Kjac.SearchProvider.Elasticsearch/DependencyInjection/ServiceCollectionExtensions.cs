using Kjac.SearchProvider.Elasticsearch.Services;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Search.Core.Services;

namespace Kjac.SearchProvider.Elasticsearch.DependencyInjection;

internal static class ServiceCollectionExtensions
{
    public static IServiceCollection AddElasticsearch(this IServiceCollection services)
    {
        // register the Elasticsearch searcher and indexer so they can be used explicitly for index registrations
        services.AddTransient<IElasticsearchIndexer, ElasticsearchIndexer>();
        services.AddTransient<IElasticsearchSearcher, ElasticsearchSearcher>();

        // register the Elasticsearch searcher and indexer as the defaults
        services.AddTransient<IIndexer, ElasticsearchIndexer>();
        services.AddTransient<ISearcher, ElasticsearchSearcher>();

        services.AddSingleton<IElasticsearchClientFactory, ElasticsearchClientFactory>();

        return services;
    }
}