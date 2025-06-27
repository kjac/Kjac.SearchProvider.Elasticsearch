using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Kjac.SearchProvider.Elasticsearch.Extensions;
using Microsoft.Extensions.Logging;
using Umbraco.Cms.Core.Sync;

namespace Kjac.SearchProvider.Elasticsearch.Services;

internal sealed class ElasticsearchIndexManager : ElasticsearchIndexManagingServiceBase, IElasticsearchIndexManager
{
    private readonly IElasticsearchClientFactory _clientFactory;
    private readonly ILogger<ElasticsearchIndexManager> _logger;
    
    public ElasticsearchIndexManager(IServerRoleAccessor serverRoleAccessor, IElasticsearchClientFactory clientFactory, ILogger<ElasticsearchIndexManager> logger)
        : base(serverRoleAccessor)
    {
        _clientFactory = clientFactory;
        _logger = logger;
    }

    public async Task EnsureAsync(string indexAlias)
    {
        if (CanManipulateIndexes())
        {
            return;
        }

        indexAlias = indexAlias.ValidIndexAlias();

        var client = _clientFactory.GetClient();
        
        var existsResponse = await client.Indices.ExistsAsync(indexAlias);
        if (existsResponse.Exists)
        {
            return;
        }

        _logger.LogInformation("Creating index {indexAlias}...", indexAlias);
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
                )
        );

        if (createResponse.Acknowledged)
        {
            _logger.LogInformation("Index {indexAlias} has been created.", indexAlias);
        }
        else
        {
            _logger.LogError("Index {indexAlias} could not be created. Debug info from Elastic: {debugInformation}", indexAlias, createResponse.DebugInformation);
        }
    }
}