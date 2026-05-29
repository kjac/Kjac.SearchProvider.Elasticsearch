using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Analysis;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Umbraco.Cms.Core.Sync;
using ExistsResponse = Elastic.Clients.Elasticsearch.IndexManagement.ExistsResponse;

namespace Kjac.SearchProvider.Elasticsearch.Services;

internal sealed class ElasticsearchIndexManager : ElasticsearchIndexManagingServiceBase, IElasticsearchIndexManager
{
    private readonly IElasticsearchClientFactory _clientFactory;
    private readonly IIndexAliasResolver _indexAliasResolver;
    private readonly ILogger<ElasticsearchIndexManager> _logger;
    private readonly IndexerOptions _indexerOptions;

    public ElasticsearchIndexManager(
        IServerRoleAccessor serverRoleAccessor,
        IElasticsearchClientFactory clientFactory,
        IIndexAliasResolver indexAliasResolver,
        IOptions<IndexerOptions> indexerOptions,
        ILogger<ElasticsearchIndexManager> logger)
        : base(serverRoleAccessor)
    {
        _clientFactory = clientFactory;
        _indexAliasResolver = indexAliasResolver;
        _indexerOptions = indexerOptions.Value;
        _logger = logger;
    }

    public async Task EnsureAsync(string indexAlias)
    {
        if (ShouldNotManipulateIndexes())
        {
            return;
        }

        indexAlias = _indexAliasResolver.Resolve(indexAlias);
        await EnsureResolvedIndexAliasAsync(indexAlias);
    }

    public async Task ResetAsync(string indexAlias)
    {
        if (ShouldNotManipulateIndexes())
        {
            return;
        }

        indexAlias = _indexAliasResolver.Resolve(indexAlias);

        ElasticsearchClient client = _clientFactory.GetClient();

        ExistsResponse existsResponse = await client.Indices.ExistsAsync(indexAlias);
        if (existsResponse.Exists)
        {
            DeleteIndexResponse result = await client.Indices.DeleteAsync(indexAlias);

            if (result.IsValidResponse is false)
            {
                LogFailedElasticResponse(_logger, indexAlias, "Could not reset the index", result);
                return;
            }
        }

        await EnsureResolvedIndexAliasAsync(indexAlias);
    }

    private async Task EnsureResolvedIndexAliasAsync(string indexAlias)
    {
        ElasticsearchClient client = _clientFactory.GetClient();

        ExistsResponse existsResponse = await client.Indices.ExistsAsync(indexAlias);
        if (existsResponse.Exists)
        {
            return;
        }

        var properties = new Dictionary<PropertyName, IProperty>
        {
            { IndexConstants.FieldNames.Key, new KeywordProperty() },
            { IndexConstants.FieldNames.ObjectType, new KeywordProperty() },
            { IndexConstants.FieldNames.Culture, new KeywordProperty() },
            { IndexConstants.FieldNames.AccessKeys, new KeywordProperty() },
        };

        if (_indexerOptions.UseSuggestions)
        {
            // the "suggest" field is analyzed with the default analyzer (used for phrase-prefix
            // matching), while the "shingle" sub-field tokenizes into word n-grams that the
            // suggestion terms aggregation buckets over - hence text + fielddata, not keyword
            properties.Add(
                IndexConstants.FieldNames.Suggest,
                new TextProperty
                {
                    Fields = new Properties(
                        new Dictionary<PropertyName, IProperty>
                        {
                            {
                                IndexConstants.Analysis.ShingleSubField,
                                new TextProperty
                                {
                                    Analyzer = IndexConstants.Analysis.ShingleAnalyzerName,
                                    Fielddata = true
                                }
                            }
                        }
                    )
                }
            );
        }

        _logger.LogInformation("Creating index {indexAlias}...", indexAlias);
        CreateIndexResponse createResponse = await client.Indices.CreateAsync(
            indexAlias,
            cd =>
            {
                cd.Mappings(
                    md => md
                        .Properties(new Properties(properties))
                        .DynamicTemplates(
                            [
                                new KeyValuePair<string, DynamicTemplate>(
                                    "keyword_fields_as_keywords",
                                    new DynamicTemplate { Mapping = new KeywordProperty(), Match = ["*_keywords"] }
                                ),
                                new KeyValuePair<string, DynamicTemplate>(
                                    "decimal_fields_as_doubles",
                                    new DynamicTemplate { Mapping = new DoubleNumberProperty(), Match = ["*_decimals"] }
                                )
                            ]
                        )
                );

                if (_indexerOptions.UseSuggestions)
                {
                    cd.Settings(
                        new IndexSettings
                        {
                            Analysis = new IndexSettingsAnalysis
                            {
                                TokenFilters = new TokenFilters
                                {
                                    {
                                        IndexConstants.Analysis.ShingleFilterName,
                                        new ShingleTokenFilter
                                        {
                                            MinShingleSize = 2,
                                            MaxShingleSize = 3,
                                            OutputUnigrams = true
                                        }
                                    }
                                },
                                Analyzers = new Analyzers
                                {
                                    {
                                        IndexConstants.Analysis.ShingleAnalyzerName,
                                        new CustomAnalyzer
                                        {
                                            Tokenizer = "standard",
                                            Filter = new[] { "lowercase", IndexConstants.Analysis.ShingleFilterName }
                                        }
                                    }
                                }
                            }
                        }
                    );
                }
            }
        );

        if (createResponse.Acknowledged)
        {
            _logger.LogInformation("Index {indexAlias} has been created.", indexAlias);
        }
        else
        {
            LogFailedElasticResponse(_logger, "Index could not be created", indexAlias, createResponse);
        }
    }
}
