using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Umbraco.Cms.Core.Events;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Search.Core.Models.Configuration;
using Umbraco.Cms.Search.Core.Services;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Kjac.SearchProvider.Elasticsearch.Extensions;
using Kjac.SearchProvider.Elasticsearch.Services;
using IndexOptions = Umbraco.Cms.Search.Core.Configuration.IndexOptions;

namespace Kjac.SearchProvider.Elasticsearch.NotificationHandlers;

internal sealed class EnsureIndexesNotificationHandler : INotificationAsyncHandler<UmbracoApplicationStartingNotification>
{
    private readonly ElasticClientFactory _clientFactory;
    private readonly IServiceProvider _serviceProvider;
    private readonly IndexOptions _indexOptions;
    private readonly ILogger<EnsureIndexesNotificationHandler> _logger;

    public EnsureIndexesNotificationHandler(
        ElasticClientFactory clientFactory,
        IServiceProvider serviceProvider,
        IOptions<IndexOptions> indexOptions,
        ILogger<EnsureIndexesNotificationHandler> logger)
    {
        _clientFactory = clientFactory;
        _serviceProvider = serviceProvider;
        _indexOptions = indexOptions.Value;
        _logger = logger;
    }

    public async Task HandleAsync(UmbracoApplicationStartingNotification notification, CancellationToken cancellationToken)
    {
        var implicitIndexServiceType = typeof(IIndexer);
        var defaultIndexServiceType = _serviceProvider.GetRequiredService<IIndexer>().GetType();
        var elasticIndexServiceType = typeof(IElasticIndexer);

        foreach (var indexRegistration in _indexOptions.GetIndexRegistrations())
        {
            var shouldEnsureIndex = indexRegistration.Indexer == elasticIndexServiceType
                || (indexRegistration.Indexer == implicitIndexServiceType && defaultIndexServiceType == elasticIndexServiceType);

            if (shouldEnsureIndex)
            {
                await EnsureIndex(indexRegistration, cancellationToken);
            }
        }
    }

    private async Task EnsureIndex(IndexRegistration indexRegistration, CancellationToken cancellationToken)
    {
        var client = _clientFactory.GetClient();

        await client.EnsureIndexAsync(indexRegistration.IndexAlias, _logger, cancellationToken);
    }
}