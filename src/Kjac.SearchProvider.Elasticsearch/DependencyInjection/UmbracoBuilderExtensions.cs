using Examine;
using Kjac.SearchProvider.Elasticsearch.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Umbraco.Cms.Core.DependencyInjection;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Search.Core.Services.ContentIndexing;
using Kjac.SearchProvider.Elasticsearch.NotificationHandlers;
using Kjac.SearchProvider.Elasticsearch.Services;
using Umbraco.Extensions;
using CoreConstants = Umbraco.Cms.Search.Core.Constants;
using IndexOptions = Umbraco.Cms.Search.Core.Configuration.IndexOptions;

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
                options.RegisterElasticsearchContentIndex<IDraftContentChangeStrategy>(
                    CoreConstants.IndexAliases.DraftContent,
                    UmbracoObjectTypes.Document
                );
                options.RegisterElasticsearchContentIndex<IPublishedContentChangeStrategy>(
                    CoreConstants.IndexAliases.PublishedContent,
                    UmbracoObjectTypes.Document
                );

                // register Elasticsearch index for media
                options.RegisterElasticsearchContentIndex<IDraftContentChangeStrategy>(
                    CoreConstants.IndexAliases.DraftMedia,
                    UmbracoObjectTypes.Media
                );

                // register Elasticsearch index for members
                options.RegisterElasticsearchContentIndex<IDraftContentChangeStrategy>(
                    CoreConstants.IndexAliases.DraftMembers,
                    UmbracoObjectTypes.Member
                );
            }
        );

        // ensure all indexes exist before Umbraco has finished start-up
        builder.AddNotificationAsyncHandler<UmbracoApplicationStartingNotification, EnsureIndexesNotificationHandler>();

        return builder;
    }

    public static IUmbracoBuilder DisableDefaultExamineIndexes(this IUmbracoBuilder builder)
    {
        builder.Services.AddSingleton<ExamineManager>();
        builder.Services.AddUnique<IExamineManager, MaskedCoreIndexesExamineManager>();

        return builder;
    }
}
