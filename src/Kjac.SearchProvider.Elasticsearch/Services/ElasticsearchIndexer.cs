using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Microsoft.Extensions.Logging;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Models.Indexing;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Kjac.SearchProvider.Elasticsearch.Extensions;
using Umbraco.Cms.Core.Sync;
using Umbraco.Cms.Search.Core.Extensions;
using Umbraco.Extensions;
using IndexField = Umbraco.Cms.Search.Core.Models.Indexing.IndexField;
using IProperty = Elastic.Clients.Elasticsearch.Mapping.IProperty;

namespace Kjac.SearchProvider.Elasticsearch.Services;

internal sealed class ElasticsearchIndexer : ElasticsearchServiceBase, IElasticsearchIndexer
{
    private readonly IElasticsearchClientFactory _clientFactory;
    private readonly IServerRoleAccessor _serverRoleAccessor;
    private readonly ILogger<ElasticsearchIndexer> _logger;

    public ElasticsearchIndexer(IElasticsearchClientFactory clientFactory, IServerRoleAccessor serverRoleAccessor, ILogger<ElasticsearchIndexer> logger)
    {
        _clientFactory = clientFactory;
        _serverRoleAccessor = serverRoleAccessor;
        _logger = logger;
    }

    public async Task AddOrUpdateAsync(string indexAlias, Guid id, UmbracoObjectTypes objectType, IEnumerable<Variation> variations, IEnumerable<IndexField> fields, ContentProtection? protection)
    {
        if (IsUnsupportedServerRole())
        {
            return;
        }

        var fieldsByFieldName = fields.GroupBy(field => field.FieldName);
        var documents = variations.Select(variation =>
        {
            // document variation
            var culture = variation.Culture.IndexCulture();
            var segment = variation.Segment.IndexSegment();

            // document access (no access maps to an empty key for querying)
            var accessKeys = protection?.AccessIds.Any() is true
                ? protection.AccessIds.ToArray()
                : [Guid.Empty];

            // relevant field values for this variation (including invariant fields)
            var variationFields = fieldsByFieldName.Select(g =>
                g.FirstOrDefault(f => f.Culture == variation.Culture && f.Segment == variation.Segment)
                ?? g.FirstOrDefault(f => variation.Culture is not null && f.Culture == variation.Culture && f.Segment is null)
                ?? g.FirstOrDefault(f => variation.Segment is not null && f.Culture is null && f.Segment == variation.Segment)
                ?? g.FirstOrDefault(f => f.Culture is null && f.Segment is null)
            ).WhereNotNull().ToArray();

            // all text fields for "free text query on all fields"
            var allTexts = variationFields
                .SelectMany(field => field.Value.Texts ?? [])
                .ToArray();

            // explicit document field values
            var fieldValues = variationFields
                .SelectMany(field =>
                {
                    return new (string FieldName, string Postfix, object[]? Values)[]
                    {
                        (field.FieldName, IndexConstants.FieldTypePostfix.Texts, field.Value.Texts?.OfType<object>().ToArray()),
                        (field.FieldName, IndexConstants.FieldTypePostfix.Integers, field.Value.Integers?.OfType<object>().ToArray()),
                        (field.FieldName, IndexConstants.FieldTypePostfix.Decimals, field.Value.Decimals?.OfType<object>().ToArray()),
                        (field.FieldName, IndexConstants.FieldTypePostfix.DateTimeOffsets, field.Value.DateTimeOffsets?.OfType<object>().ToArray()),
                        (field.FieldName, IndexConstants.FieldTypePostfix.Keywords, field.Value.Keywords?.OfType<object>().ToArray())
                    };
                })
                .Where(f => f.Values?.Any() is true)
                .ToDictionary(f => $"{f.FieldName}{f.Postfix}", f => f.Values!);

            return new IndexDocument
            {
                Id = $"{id:D}.{culture}.{segment}",
                ObjectType = objectType.ToString(),
                Key = id,
                Culture = culture,
                Segment = segment,
                AccessKeys = accessKeys,
                AllTexts = allTexts,
                Fields = fieldValues
            };
        });

        var client = _clientFactory.GetClient();

        var response = await client.IndexManyAsync(documents, index: indexAlias.ValidIndexAlias());
        if (response.IsValidResponse is false)
        {
            LogFailedElasticResponse(_logger, indexAlias, "Could not perform add/update", response);
        }
    }

    public async Task DeleteAsync(string indexAlias, IEnumerable<Guid> ids)
    {
        if (IsUnsupportedServerRole())
        {
            return;
        }

        var client = _clientFactory.GetClient();
        var result = await client.DeleteByQueryAsync<IndexDocument>(dr => dr
            .Indices(indexAlias.ValidIndexAlias())
            .Query(qd => qd
                .Terms(td => td
                    .Field(FieldName(Umbraco.Cms.Search.Core.Constants.FieldNames.PathIds, IndexConstants.FieldTypePostfix.Keywords))
                    .Terms(new TermsQueryField(ids.Select(key => FieldValue.String(key.AsKeyword())).ToArray())))
            )
        );

        if (result.IsValidResponse is false)
        {
            LogFailedElasticResponse(_logger, indexAlias, "Could not perform delete", result);
        }
    }

    public async Task ResetAsync(string indexAlias)
    {
        if (IsUnsupportedServerRole())
        {
            return;
        }

        var client = _clientFactory.GetClient();
        var existsResponse = await client.Indices.ExistsAsync(indexAlias);
        if (existsResponse.Exists is false)
        {
            return;
        }

        var result = await client.Indices.DeleteAsync(indexAlias);

        if (result.IsValidResponse is false)
        {
            LogFailedElasticResponse(_logger, indexAlias, "Could not reset the index", result);
        }
    }

    public async Task EnsureAsync(string indexAlias)
    {
        if (IsUnsupportedServerRole())
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
    
    private bool IsUnsupportedServerRole() => _serverRoleAccessor.CurrentServerRole is ServerRole.Subscriber;
    
    private record IndexDocument
    {
        [JsonPropertyName(IndexConstants.FieldNames.Id)]
        public required string Id { get; init; }

        [JsonPropertyName(IndexConstants.FieldNames.ObjectType)]
        public required string? ObjectType { get; init; }

        [JsonPropertyName(IndexConstants.FieldNames.Key)]
        public required Guid Key { get; init; }

        [JsonPropertyName(IndexConstants.FieldNames.Culture)]
        public required string Culture { get; init; }

        [JsonPropertyName(IndexConstants.FieldNames.Segment)]
        public required string Segment { get; init; }

        [JsonPropertyName(IndexConstants.FieldNames.AccessKeys)]
        public required Guid[] AccessKeys { get; init; }

        [JsonPropertyName(IndexConstants.FieldNames.AllTexts)]
        public required string[] AllTexts { get; init; }

        [JsonPropertyName(IndexConstants.FieldNames.Fields)]
        public required Dictionary<string, object[]> Fields { get; init; }
    }
}