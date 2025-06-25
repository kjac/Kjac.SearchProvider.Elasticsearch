using Kjac.SearchProvider.Elasticsearch.DependencyInjection;
using Umbraco.Cms.Core.Composing;

namespace Kjac.SearchProvider.Elasticsearch.Site.DependencyInjection;

public class SiteComposer : IComposer
{
    public void Compose(IUmbracoBuilder builder)
        => builder.AddElasticsearchSearchProvider();
}