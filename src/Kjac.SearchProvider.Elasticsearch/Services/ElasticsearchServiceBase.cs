using Elastic.Transport.Products.Elasticsearch;
using Microsoft.Extensions.Logging;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Umbraco.Extensions;

namespace Kjac.SearchProvider.Elasticsearch.Services;

internal abstract class ElasticsearchServiceBase
{
    protected static string FieldName(string fieldName, string postfix)
        => $"{IndexConstants.FieldNames.Fields}.{fieldName}{postfix}";

    protected static string SegmentedField(string fieldName, string? segment)
        => segment.IsNullOrWhiteSpace() ? fieldName : $"__{segment}_{fieldName}";

    protected void LogFailedElasticResponse(
        ILogger logger,
        string indexAlias,
        string message,
        ElasticsearchResponse response)
        => logger.LogError(
            "{message}. Elastic index: {indexAlias}. Debug info from Elastic: {debugInformation}",
            message,
            indexAlias,
            response.DebugInformation
        );
}
