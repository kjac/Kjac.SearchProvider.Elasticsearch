using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.DependencyInjection;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Search.Core.Configuration;
using Umbraco.Cms.Search.Core.Services.ContentIndexing;
using Kjac.SearchProvider.Elasticsearch.NotificationHandlers;
using Kjac.SearchProvider.Elasticsearch.Services;
using CoreConstants = Umbraco.Cms.Search.Core.Constants;

namespace Kjac.SearchProvider.Elasticsearch.DependencyInjection;

public static class UmbracoBuilderExtensions
{
    public static IUmbracoBuilder AddElasticsearchSearchProvider(this IUmbracoBuilder builder)
    {
        builder.Services.AddElasticsearch(builder.Config);

        builder.Services.Configure<IndexOptions>(
            options =>
            {
                // register Elasticsearch indexes for draft and published content
                options.RegisterContentIndex<IElasticsearchIndexer, IElasticsearchSearcher, IDraftContentChangeStrategy>(
                    CoreConstants.IndexAliases.DraftContent,
                    UmbracoObjectTypes.Document
                );
                options.RegisterContentIndex<IElasticsearchIndexer, IElasticsearchSearcher, IPublishedContentChangeStrategy>(
                    CoreConstants.IndexAliases.PublishedContent,
                    UmbracoObjectTypes.Document
                );

                // register Elasticsearch index for media
                options.RegisterContentIndex<IElasticsearchIndexer, IElasticsearchSearcher, IDraftContentChangeStrategy>(
                    CoreConstants.IndexAliases.DraftMedia,
                    UmbracoObjectTypes.Media
                );

                // register Elasticsearch index for members
                options.RegisterContentIndex<IElasticsearchIndexer, IElasticsearchSearcher, IDraftContentChangeStrategy>(
                    CoreConstants.IndexAliases.DraftMembers,
                    UmbracoObjectTypes.Member
                );
            }
        );

        // ensure all indexes exist before Umbraco has finished start-up
        builder.AddNotificationAsyncHandler<UmbracoApplicationStartingNotification, EnsureIndexesNotificationHandler>();

        return builder;
    }
}
