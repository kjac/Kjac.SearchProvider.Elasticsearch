using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Umbraco.Cms.Core.Events;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Search.Core.Services;
using Kjac.SearchProvider.Elasticsearch.Services;
using IndexOptions = Umbraco.Cms.Search.Core.Configuration.IndexOptions;

namespace Kjac.SearchProvider.Elasticsearch.NotificationHandlers;

internal sealed class EnsureIndexesNotificationHandler : INotificationAsyncHandler<UmbracoApplicationStartingNotification>
{
    private readonly IElasticsearchIndexer _elasticsearchIndexer;
    private readonly IServiceProvider _serviceProvider;
    private readonly IndexOptions _indexOptions;

    public EnsureIndexesNotificationHandler(
        IElasticsearchIndexer elasticsearchIndexer,
        IServiceProvider serviceProvider,
        IOptions<IndexOptions> indexOptions)
    {
        _serviceProvider = serviceProvider;
        _indexOptions = indexOptions.Value;
        _elasticsearchIndexer = elasticsearchIndexer;
    }

    public async Task HandleAsync(UmbracoApplicationStartingNotification notification, CancellationToken cancellationToken)
    {
        var implicitIndexServiceType = typeof(IIndexer);
        var defaultIndexServiceType = _serviceProvider.GetRequiredService<IIndexer>().GetType();
        var elasticIndexServiceType = typeof(IElasticsearchIndexer);

        foreach (var indexRegistration in _indexOptions.GetIndexRegistrations())
        {
            var shouldEnsureIndex = indexRegistration.Indexer == elasticIndexServiceType
                || (indexRegistration.Indexer == implicitIndexServiceType && defaultIndexServiceType == elasticIndexServiceType);

            if (shouldEnsureIndex)
            {
                await _elasticsearchIndexer.EnsureAsync(indexRegistration.IndexAlias);
            }
        }
    }
}