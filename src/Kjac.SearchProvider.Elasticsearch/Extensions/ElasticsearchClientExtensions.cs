using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Microsoft.Extensions.Logging;

namespace Kjac.SearchProvider.Elasticsearch.Extensions;

internal static class ElasticsearchClientExtensions
{
    public static async Task<bool> EnsureIndexAsync(this ElasticsearchClient client, string indexAlias, ILogger logger, CancellationToken cancellationToken)
    {
        indexAlias = indexAlias.ValidIndexAlias();
        var existsResponse = await client.Indices.ExistsAsync(Indices.Index(indexAlias), cancellationToken);
        if (existsResponse.Exists)
        {
            return true;
        }

        logger.LogInformation("Creating index {indexAlias}...", indexAlias);

        var createResponse = await client.Indices.CreateAsync(
            indexAlias,
            cd => cd
                .Mappings(md => md
                    .Properties(
                        new Properties(new Dictionary<PropertyName, IProperty>
                            {
                                { IndexConstants.FieldNames.Key, new KeywordProperty() },
                                { IndexConstants.FieldNames.ObjectType, new KeywordProperty() },
                                { IndexConstants.FieldNames.Culture, new KeywordProperty() },
                                { IndexConstants.FieldNames.Segment, new KeywordProperty() },
                                { IndexConstants.FieldNames.AccessKeys, new KeywordProperty() },
                            }
                        ))
                    .DynamicTemplates([
                            new KeyValuePair<string, DynamicTemplate>(
                                "keyword_fields_as_keywords",
                                new DynamicTemplate
                                {
                                    Mapping = new KeywordProperty(),
                                    MatchMappingType = ["string"],
                                    Match = ["*_keywords"]
                                }
                            )
                        ]
                    )
                ),
            cancellationToken
        );

        if (createResponse.Acknowledged is false)
        {
            logger.LogError("Index {indexAlias} could not be created. Debug info from Elastic: {debugInformation}", indexAlias, createResponse.DebugInformation);
            return false;
        }
        
        logger.LogInformation("Index {indexAlias} has been created.", indexAlias);
        return true;
    }
}