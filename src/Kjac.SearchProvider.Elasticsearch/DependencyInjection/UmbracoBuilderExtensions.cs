using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.DependencyInjection;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Search.Core.Configuration;
using Umbraco.Cms.Search.Core.Services.ContentIndexing;
using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.NotificationHandlers;
using Kjac.SearchProvider.Elasticsearch.Services;
using CoreConstants = Umbraco.Cms.Search.Core.Constants;

namespace Kjac.SearchProvider.Elasticsearch.DependencyInjection;

public static class UmbracoBuilderExtensions
{
    public static IUmbracoBuilder AddElasticSearchProvider(this IUmbracoBuilder builder)
    {
        builder.Services.AddElastic();

        builder.Services.Configure<ElasticClient>(builder.Config.GetSection(nameof(ElasticClient)));

        builder.Services.Configure<IndexOptions>(options =>
        {
            // register Elastic indexes for draft and published content
            options.RegisterIndex<IElasticIndexer, IElasticSearcher, IDraftContentChangeStrategy>(CoreConstants.IndexAliases.DraftContent, UmbracoObjectTypes.Document);
            options.RegisterIndex<IElasticIndexer, IElasticSearcher, IPublishedContentChangeStrategy>(CoreConstants.IndexAliases.PublishedContent, UmbracoObjectTypes.Document);

            // register Elastic index for media
            options.RegisterIndex<IElasticIndexer, IElasticSearcher, IDraftContentChangeStrategy>(CoreConstants.IndexAliases.DraftMedia, UmbracoObjectTypes.Media);

            // register Elastic index for members
            options.RegisterIndex<IElasticIndexer, IElasticSearcher, IDraftContentChangeStrategy>(CoreConstants.IndexAliases.DraftMembers, UmbracoObjectTypes.Member);
        });

        // ensure all indexes exist before Umbraco has finished start-up
        builder.AddNotificationAsyncHandler<UmbracoApplicationStartingNotification, EnsureIndexesNotificationHandler>();

        return builder;
    }
}
