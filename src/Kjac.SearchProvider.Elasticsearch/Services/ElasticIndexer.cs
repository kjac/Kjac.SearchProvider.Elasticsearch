using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Microsoft.Extensions.Logging;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Models.Indexing;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Kjac.SearchProvider.Elasticsearch.Extensions;
using Umbraco.Cms.Search.Core.Extensions;
using Umbraco.Extensions;

namespace Kjac.SearchProvider.Elasticsearch.Services;

// TODO: this service should never execute anything on Subscriber instances

internal sealed class ElasticIndexer : ElasticServiceBase, IElasticIndexer
{
    private readonly ElasticClientFactory _clientFactory;
    private readonly ILogger<ElasticIndexer> _logger;

    public ElasticIndexer(ElasticClientFactory clientFactory, ILogger<ElasticIndexer> logger)
    {
        _clientFactory = clientFactory;
        _logger = logger;
    }

    public async Task AddOrUpdateAsync(string indexAlias, Guid id, UmbracoObjectTypes objectType, IEnumerable<Variation> variations, IEnumerable<IndexField> fields, ContentProtection? protection)
    {
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

    public Task ResetAsync(string indexAlias)
    {
        // TODO: implement
        return Task.CompletedTask;
    }

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