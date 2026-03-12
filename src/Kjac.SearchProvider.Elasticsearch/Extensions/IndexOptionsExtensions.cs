using Kjac.SearchProvider.Elasticsearch.Services;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Configuration;
using Umbraco.Cms.Search.Core.Services.ContentIndexing;

namespace Kjac.SearchProvider.Elasticsearch.Extensions;

public static class IndexOptionsExtensions
{
    public static IndexOptions RegisterElasticsearchContentIndex<TContentChangeStrategy>(
        this IndexOptions indexOptions,
        string indexAlias,
        params UmbracoObjectTypes[] containedObjectTypes)
        where TContentChangeStrategy : class, IContentChangeStrategy
    {
        indexOptions.RegisterContentIndex<IElasticsearchIndexer, IElasticsearchSearcher, TContentChangeStrategy>(
            indexAlias,
            sameOriginOnly: true,
            containedObjectTypes
        );
        return indexOptions;
    }
}
