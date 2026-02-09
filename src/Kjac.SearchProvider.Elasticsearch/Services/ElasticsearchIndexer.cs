using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Microsoft.Extensions.Logging;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Models.Indexing;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Kjac.SearchProvider.Elasticsearch.Extensions;
using Umbraco.Cms.Core.Sync;
using Umbraco.Cms.Search.Core.Extensions;
using Umbraco.Extensions;
using HealthStatus = Umbraco.Cms.Search.Core.Models.Indexing.HealthStatus;
using IndexField = Umbraco.Cms.Search.Core.Models.Indexing.IndexField;

namespace Kjac.SearchProvider.Elasticsearch.Services;

internal sealed class ElasticsearchIndexer : ElasticsearchIndexManagingServiceBase, IElasticsearchIndexer
{
    private readonly IElasticsearchClientFactory _clientFactory;
    private readonly IElasticsearchIndexManager _indexManager;
    private readonly IIndexAliasResolver _indexAliasResolver;
    private readonly ILogger<ElasticsearchIndexer> _logger;

    public ElasticsearchIndexer(
        IServerRoleAccessor serverRoleAccessor,
        IElasticsearchIndexManager indexManager,
        IElasticsearchClientFactory clientFactory,
        IIndexAliasResolver indexAliasResolver,
        ILogger<ElasticsearchIndexer> logger)
        : base(serverRoleAccessor)
    {
        _clientFactory = clientFactory;
        _indexManager = indexManager;
        _indexAliasResolver = indexAliasResolver;
        _logger = logger;
    }

    public async Task AddOrUpdateAsync(
        string indexAlias,
        Guid id,
        UmbracoObjectTypes objectType,
        IEnumerable<Variation> variations,
        IEnumerable<IndexField> fields,
        ContentProtection? protection)
    {
        if (ShouldNotManipulateIndexes())
        {
            return;
        }

        IEnumerable<IGrouping<string, IndexField>> fieldsByFieldName = fields.GroupBy(field => field.FieldName);
        IEnumerable<IndexDocument> documents = variations.Select(
            variation =>
            {
                // document variation
                var culture = variation.Culture.IndexCulture();

                // document access (no access maps to an empty key for querying)
                Guid[] accessKeys = protection?.AccessIds.Any() is true
                    ? protection.AccessIds.ToArray()
                    : [Guid.Empty];

                // relevant field values for this variation (including invariant fields)
                IndexField[] variationFields = fieldsByFieldName.SelectMany(
                        g =>
                        {
                            IndexField[] applicableFields = g
                                .Where(f => f.Culture is null || f.Culture == variation.Culture)
                                .ToArray();

                            return applicableFields.Any()
                                ? applicableFields
                                    .GroupBy(field => field.Segment)
                                    .Select(segmentFields => new IndexField(
                                        SegmentedField(g.Key, segmentFields.Key),
                                        new IndexValue
                                        {
                                            DateTimeOffsets = segmentFields.SelectMany(f => f.Value.DateTimeOffsets ?? []).NullIfEmpty(),
                                            Decimals = segmentFields.SelectMany(f => f.Value.Decimals ?? []).NullIfEmpty(),
                                            Integers = segmentFields.SelectMany(f => f.Value.Integers ?? []).NullIfEmpty(),
                                            Keywords = segmentFields.SelectMany(f => f.Value.Keywords ?? []).NullIfEmpty(),
                                            Texts = segmentFields.SelectMany(f => f.Value.Texts ?? []).NullIfEmpty(),
                                            TextsR1 = segmentFields.SelectMany(f => f.Value.TextsR1 ?? []).NullIfEmpty(),
                                            TextsR2 = segmentFields.SelectMany(f => f.Value.TextsR2 ?? []).NullIfEmpty(),
                                            TextsR3 = segmentFields.SelectMany(f => f.Value.TextsR3 ?? []).NullIfEmpty(),
                                        },
                                        variation.Culture,
                                        segmentFields.Key
                                    ))
                                : [];
                        }
                    )
                    .ToArray();

                // explicit document field values
                var fieldValues = variationFields
                    .SelectMany(
                        field =>
                        {
                            return new (string FieldName, string Postfix, object[]? Values)[]
                            {
                                (
                                    field.FieldName,
                                    IndexConstants.FieldTypePostfix.Texts,
                                    field.Value.Texts?.OfType<object>().ToArray()
                                ),
                                (
                                    field.FieldName,
                                    IndexConstants.FieldTypePostfix.TextsR1,
                                    field.Value.TextsR1?.OfType<object>().ToArray()
                                ),
                                (
                                    field.FieldName,
                                    IndexConstants.FieldTypePostfix.TextsR2,
                                    field.Value.TextsR2?.OfType<object>().ToArray()
                                ),
                                (
                                    field.FieldName,
                                    IndexConstants.FieldTypePostfix.TextsR3,
                                    field.Value.TextsR3?.OfType<object>().ToArray()
                                ),
                                (
                                    field.FieldName,
                                    IndexConstants.FieldTypePostfix.Integers,
                                    field.Value.Integers?.OfType<object>().ToArray()
                                ),
                                (
                                    field.FieldName,
                                    IndexConstants.FieldTypePostfix.Decimals,
                                    field.Value.Decimals?.OfType<object>().ToArray()
                                ),
                                (
                                    field.FieldName,
                                    IndexConstants.FieldTypePostfix.DateTimeOffsets,
                                    field.Value.DateTimeOffsets?.OfType<object>().ToArray()
                                ),
                                (
                                    field.FieldName,
                                    IndexConstants.FieldTypePostfix.Keywords,
                                    field.Value.Keywords?.OfType<object>().ToArray()
                                )
                            };
                        }
                    )
                    .Where(f => f.Values?.Any() is true)
                    .ToDictionary(f => $"{f.FieldName}{f.Postfix}", f => f.Values!);

                IndexField[] textFields = variationFields
                    .Where(f => f.Value.Texts is not null
                                || f.Value.TextsR1 is not null
                                || f.Value.TextsR2 is not null
                                || f.Value.TextsR3 is not null
                    )
                    .ToArray();

                // all text fields for "free text query on all fields"
                IndexField[] defaultSegmentTextFields = textFields.Where(f => f.Segment is null).ToArray();
                var allTexts = defaultSegmentTextFields
                    .SelectMany(field => field.Value.Texts ?? [])
                    .ToArray();
                var allTextsR1 = defaultSegmentTextFields
                    .SelectMany(field => field.Value.TextsR1 ?? [])
                    .ToArray();
                var allTextsR2 = defaultSegmentTextFields
                    .SelectMany(field => field.Value.TextsR2 ?? [])
                    .ToArray();
                var allTextsR3 = defaultSegmentTextFields
                    .SelectMany(field => field.Value.TextsR3 ?? [])
                    .ToArray();

                if (allTexts.Length > 0)
                {
                    fieldValues.Add(IndexConstants.FieldNames.AllTexts, [string.Join(" ", allTexts).ToLowerInvariant()]);
                }

                if (allTextsR1.Length > 0)
                {
                    fieldValues.Add(IndexConstants.FieldNames.AllTextsR1, [string.Join(" ", allTextsR1).ToLowerInvariant()]);
                }

                if (allTextsR2.Length > 0)
                {
                    fieldValues.Add(IndexConstants.FieldNames.AllTextsR2, [string.Join(" ", allTextsR2).ToLowerInvariant()]);
                }

                if (allTextsR3.Length > 0)
                {
                    fieldValues.Add(IndexConstants.FieldNames.AllTextsR3, [string.Join(" ", allTextsR3).ToLowerInvariant()]);
                }

                // all text fields for "free text query on all fields" (segment values)
                foreach (IGrouping<string?, IndexField> textFieldsBySegment in textFields.Except(defaultSegmentTextFields).GroupBy(f => f.Segment))
                {
                    var allTextsForSegment = textFieldsBySegment
                        .SelectMany(field => field.Value.Texts ?? [])
                        .ToArray();
                    var allTextsR1ForSegment = textFieldsBySegment
                        .SelectMany(field => field.Value.TextsR1 ?? [])
                        .ToArray();
                    var allTextsR2ForSegment = textFieldsBySegment
                        .SelectMany(field => field.Value.TextsR2 ?? [])
                        .ToArray();
                    var allTextsR3ForSegment = textFieldsBySegment
                        .SelectMany(field => field.Value.TextsR3 ?? [])
                        .ToArray();

                    if (allTextsForSegment.Length > 0)
                    {
                        fieldValues.Add(
                            SegmentedField(IndexConstants.FieldNames.AllTexts, textFieldsBySegment.Key),
                            [string.Join(" ", allTextsForSegment.Union(allTexts)).ToLowerInvariant()]
                        );
                    }

                    if (allTextsR1.Length > 0)
                    {
                        fieldValues.Add(
                            SegmentedField(IndexConstants.FieldNames.AllTextsR1, textFieldsBySegment.Key),
                            [string.Join(" ", allTextsR1ForSegment.Union(allTextsR1)).ToLowerInvariant()]
                        );
                    }

                    if (allTextsR2.Length > 0)
                    {
                        fieldValues.Add(
                            SegmentedField(IndexConstants.FieldNames.AllTextsR2, textFieldsBySegment.Key),
                            [string.Join(" ", allTextsR2ForSegment.Union(allTexts)).ToLowerInvariant()]
                        );
                    }

                    if (allTextsR3.Length > 0)
                    {
                        fieldValues.Add(
                            SegmentedField(IndexConstants.FieldNames.AllTextsR3, textFieldsBySegment.Key),
                            [string.Join(" ", allTextsR3ForSegment.Union(allTextsR3)).ToLowerInvariant()]
                        );
                    }
                }

                // add explicit fields for textual sorting across multiple relevance levels of text
                foreach (IndexField field in defaultSegmentTextFields)
                {
                    var sortableTexts = (field.Value.TextsR1 ?? [])
                        .Union(field.Value.TextsR2 ?? [])
                        .Union(field.Value.TextsR3 ?? [])
                        .Union(field.Value.Texts ?? [])
                        .Take(5).ToArray();
                    if (sortableTexts.Length > 0)
                    {
                        fieldValues.Add(
                            $"{field.FieldName}{IndexConstants.FieldTypePostfix.Texts}{IndexConstants.FieldTypePostfix.Sortable}",
                            [string.Join(" ", sortableTexts).ToLowerInvariant()]
                        );
                    }
                }

                return new IndexDocument
                {
                    Id = $"{id:D}.{culture}",
                    ObjectType = objectType.ToString(),
                    Key = id,
                    Culture = culture,
                    AccessKeys = accessKeys,
                    Fields = fieldValues
                };
            }
        );

        ElasticsearchClient client = _clientFactory.GetClient();

        BulkResponse response = await client.IndexManyAsync(documents, index: _indexAliasResolver.Resolve(indexAlias));
        if (response.IsValidResponse is false)
        {
            LogFailedElasticResponse(_logger, indexAlias, "Could not perform add/update", response);
        }
    }

    public async Task DeleteAsync(string indexAlias, IEnumerable<Guid> ids)
    {
        if (ShouldNotManipulateIndexes())
        {
            return;
        }

        indexAlias = _indexAliasResolver.Resolve(indexAlias);

        ElasticsearchClient client = _clientFactory.GetClient();
        DeleteByQueryResponse result = await client.DeleteByQueryAsync<IndexDocument>(
            dr => dr
                .Indices(indexAlias)
                .Query(
                    qd => qd
                        .Terms(
                            td => td
                                .Field(
                                    FieldName(
                                        Umbraco.Cms.Search.Core.Constants.FieldNames.PathIds,
                                        IndexConstants.FieldTypePostfix.Keywords
                                    )
                                )
                                .Terms(
                                    new TermsQueryField(ids.Select(key => FieldValue.String(key.AsKeyword())).ToArray())
                                )
                        )
                )
        );

        if (result.IsValidResponse is false)
        {
            LogFailedElasticResponse(_logger, indexAlias, "Could not perform delete", result);
        }
    }

    public async Task ResetAsync(string indexAlias)
        => await _indexManager.ResetAsync(indexAlias);

    public async Task<IndexMetadata> GetMetadataAsync(string indexAlias)
    {
        indexAlias = _indexAliasResolver.Resolve(indexAlias);

        Indices indices = indexAlias;
        IndicesStatsResponse statsResponse = await _clientFactory.GetClient().Indices.StatsAsync(indices, _ => {}, CancellationToken.None);

        if (statsResponse.IsValidResponse && statsResponse.Indices?.TryGetValue(indexAlias, out IndicesStats? indexStats) is true)
        {
            var documentCount = indexStats.Total?.Docs?.Count ?? 0;

            HealthStatus healthStatus = indexStats.Health switch
            {
                Elastic.Clients.Elasticsearch.HealthStatus.Green or Elastic.Clients.Elasticsearch.HealthStatus.Yellow
                    => documentCount > 0 ? HealthStatus.Healthy : HealthStatus.Empty,
                Elastic.Clients.Elasticsearch.HealthStatus.Red => HealthStatus.Corrupted,
                _ => HealthStatus.Unknown
            };

            return new IndexMetadata(documentCount, healthStatus);
        }

        return new IndexMetadata(0, HealthStatus.Unknown);
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

        [JsonPropertyName(IndexConstants.FieldNames.AccessKeys)]
        public required Guid[] AccessKeys { get; init; }

        [JsonPropertyName(IndexConstants.FieldNames.Fields)]
        public required Dictionary<string, object[]> Fields { get; init; }
    }
}
