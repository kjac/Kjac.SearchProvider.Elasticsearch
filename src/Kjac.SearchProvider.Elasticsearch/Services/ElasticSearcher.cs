using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.Fluent;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Kjac.SearchProvider.Elasticsearch.Configuration;
using Microsoft.Extensions.Logging;
using Umbraco.Cms.Core;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Models.Searching;
using Umbraco.Cms.Search.Core.Models.Searching.Faceting;
using Umbraco.Cms.Search.Core.Models.Searching.Filtering;
using Umbraco.Cms.Search.Core.Models.Searching.Sorting;
using Kjac.SearchProvider.Elasticsearch.Constants;
using Kjac.SearchProvider.Elasticsearch.Extensions;
using Microsoft.Extensions.Options;
using Umbraco.Extensions;

namespace Kjac.SearchProvider.Elasticsearch.Services;

internal sealed class ElasticSearcher : ElasticServiceBase, IElasticSearcher
{
    private readonly IElasticClientFactory _clientFactory;
    private readonly ILogger<ElasticSearcher> _logger;
    private readonly int _maxFacetValues;

    public ElasticSearcher(IElasticClientFactory clientFactory, IOptions<ElasticClient> options, ILogger<ElasticSearcher> logger)
    {
        _clientFactory = clientFactory;
        _maxFacetValues = options.Value.MaxFacetValues;
        _logger = logger;
    }

    public async Task<SearchResult> SearchAsync(
        string indexAlias,
        string? query,
        IEnumerable<Filter>? filters,
        IEnumerable<Facet>? facets,
        IEnumerable<Sorter>? sorters,
        string? culture,
        string? segment,
        AccessContext? accessContext,
        int skip,
        int take)
    {
        var client = _clientFactory.GetClient();

        // add variance filters
        var indexCultures = culture is null
            ? new[] { IndexConstants.Variation.InvariantCulture }
            : new[] { culture.IndexCulture(), IndexConstants.Variation.InvariantCulture };
        var mustFilters = new List<Action<QueryDescriptor<SearchResultDocument>>>
        {
            qd => qd.Terms(td => td
                .Field(IndexConstants.FieldNames.Culture)
                .Terms(new TermsQueryField(indexCultures.Select(FieldValue.String).ToArray()))),
            qd => qd.Term(td => td
                .Field(IndexConstants.FieldNames.Segment)
                .Value(segment.IndexSegment()))
        };

        // add protected access filter
        var accessKeys = accessContext is null
            ? [Guid.Empty]
            : new[] { Guid.Empty, accessContext.PrincipalId }.Union(accessContext.GroupIds ?? []).ToArray();
        mustFilters.Add(qd => qd.Terms(td => td
            .Field(IndexConstants.FieldNames.AccessKeys)
            .Terms(new TermsQueryField(accessKeys.Select(key => FieldValue.String(key.ToString("D"))).ToArray())))
        );

        // add full text search filter (bool prefix, AND)
        if (query.IsNullOrWhiteSpace() is false)
        {
            mustFilters.Add(qd => qd
                .MatchBoolPrefix(mq => mq
                    .Field(IndexConstants.FieldNames.AllTexts)
                    .Query(query)
                    .Operator(Operator.And)
                )
            );
        }

        // filters needs splitting into two parts; regular filters (not used for faceting) and facet filters
        // - regular filters must be applied before any facets are calculated (they narrow down the potential result set)
        // - facet filters must be applied after facets calculation has begun (additional considerations apply, see comments below)
        var filtersAsArray = filters as Filter[] ?? filters?.ToArray() ?? [];
        var facetsAsArray = facets as Facet[] ?? facets?.ToArray() ?? [];
        var facetFieldNames = facetsAsArray.Select(facet => facet.FieldName).ToArray();
        var facetFilters = filtersAsArray.Where(f => facetFieldNames.InvariantContains(f.FieldName)).ToArray();
        var regularFilters = filtersAsArray.Except(facetFilters).ToArray();

        // add regular filters
        mustFilters.AddRange(regularFilters.Where(filter => filter.Negate is false).Select(FilterDescriptor));
        var mustNotFilters = regularFilters.Where(filter => filter.Negate).Select(FilterDescriptor).ToArray();

        // add post filters for facets
        var mustPostFilters = facetFilters.Where(filter => filter.Negate is false).Select(FilterDescriptor).ToArray();
        var mustNotPostFilters = facetFilters.Where(filter => filter.Negate).Select(FilterDescriptor).ToArray();
        
        var result = await client.SearchAsync<SearchResultDocument>(sr => sr
            .Indices(indexAlias.ValidIndexAlias())
            .From(skip)
            .Size(take)
            .Query(qd => qd
                .Bool(bd => bd
                    .Must(mustFilters.ToArray())
                    .MustNot(mustNotFilters)
                )
            )
            .Aggregations(a =>
                {
                    foreach (var facet in facetsAsArray)
                    {
                        AddAggregationDescriptor(a, facet, facetFilters);
                    }
                }
            )
            .PostFilter(qd => qd
                .Bool(bd => bd
                    .Must(mustPostFilters)
                    .MustNot(mustNotPostFilters)
                )
            )
            .Sort((sorters ?? [new ScoreSorter(Direction.Descending)]).Select(SortOptionsDescriptor).ToArray())
            .Source(new SourceConfig(false))
            .Fields(
                new FieldAndFormat { Field = IndexConstants.FieldNames.Key },
                new FieldAndFormat { Field = IndexConstants.FieldNames.ObjectType }
            )
        );

        if (result.IsValidResponse is false)
        {
            LogFailedElasticResponse(_logger, indexAlias, "Could not execute a search", result);
            return new SearchResult(0, [], []);
        }

        var keys = ExtractDocuments(result);
        var facetResult = ExtractFacetResult(facetsAsArray, result.Aggregations);

        return new SearchResult(result.Total, keys, facetResult);
    }

    private void AddAggregationDescriptor(FluentDictionaryOfStringAggregation<SearchResultDocument> aggs, Facet facet, Filter[] facetFilters)
    {
        facetFilters = facetFilters.Where(f => f.FieldName != facet.FieldName).ToArray();
        if (facetFilters.Length is 0)
        {
            Inner(aggs, facet);
            return;
        }
                
        var facetMustFilters = facetFilters.Where(filter => filter.Negate is false).Select(FilterDescriptor).ToArray();
        var facetMustNotFilters = facetFilters.Where(filter => filter.Negate).Select(FilterDescriptor).ToArray();

        aggs.Add(facet.FieldName, ad => ad
            .Filter(qd => qd.Bool(bd => bd.Must(facetMustFilters).MustNot(facetMustNotFilters)))
            .Aggregations(a =>
            {
                Inner(a, facet);
            })
        );
        
        return;

        void Inner(FluentDictionaryOfStringAggregation<SearchResultDocument> aggs, Facet facet)
        {
            // TODO: try/catch to log duplicate keys
            switch(facet)
            {
                case KeywordFacet:
                case IntegerExactFacet: 
                case DecimalExactFacet:
                case DateTimeOffsetExactFacet:
                    aggs.Add(FacetName(facet), ad => ad.Terms(td => td.Field(FieldName(facet)).Size(_maxFacetValues)));
                    break;
                case IntegerRangeFacet integerRangeFacet:
                    if (integerRangeFacet.Ranges.Length == 0)
                    {
                        _logger.LogWarning("The range facet for field \"{FieldName}\" had no ranges defined, so it was skipped.", integerRangeFacet.FieldName);
                        break;
                    }

                    aggs.Add(
                        FacetName(facet),
                        ad => ad
                            .Range(rd => rd
                                .Field(FieldName(facet))
                                .Ranges(integerRangeFacet.Ranges
                                    .Select(r =>
                                        new AggregationRange
                                        {
                                            Key = r.Key,
                                            From = r.Min,
                                            To = r.Max,
                                        }
                                    )
                                    .ToArray()
                                )
                            )
                    );
                    break;
                case DecimalRangeFacet decimalRangeFacet:
                    if (decimalRangeFacet.Ranges.Length == 0)
                    {
                        _logger.LogWarning("The range facet for field \"{FieldName}\" had no ranges defined, so it was skipped.", decimalRangeFacet.FieldName);
                        break;
                    }

                    aggs.Add(
                        FacetName(facet),
                        ad => ad
                            .Range(rd => rd
                                .Field(FieldName(facet))
                                .Ranges(decimalRangeFacet.Ranges
                                    .Select(r =>
                                        new AggregationRange
                                        {
                                            Key = r.Key,
                                            From = r.Min.HasValue ? Convert.ToDouble(r.Min.Value) : null,
                                            To = r.Max.HasValue ? Convert.ToDouble(r.Max.Value) : null
                                        }
                                    )
                                    .ToArray()
                                )
                            )
                    );
                    break;
                case DateTimeOffsetRangeFacet dateTimeOffsetRangeFacet:
                    if (dateTimeOffsetRangeFacet.Ranges.Length == 0)
                    {
                        _logger.LogWarning("The range facet for field \"{FieldName}\" had no ranges defined, so it was skipped.", dateTimeOffsetRangeFacet.FieldName);
                        break;
                    }

                    aggs.Add(
                        FacetName(facet),
                        ad => ad
                            .Range(rd => rd
                                .Field(FieldName(facet))
                                .Ranges(dateTimeOffsetRangeFacet.Ranges
                                    .Select(r =>
                                        new AggregationRange
                                        {
                                            Key = r.Key,
                                            From = r.Min?.ToUnixTimeMilliseconds(),
                                            To = r.Max?.ToUnixTimeMilliseconds()
                                        }
                                    )
                                    .ToArray()
                                )
                            )
                    );
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(facet), $"Encountered an unsupported filter type: {facet.GetType().Name}");
            }
        }
    }

    private Document[] ExtractDocuments(SearchResponse<SearchResultDocument> searchResult)
        => searchResult.Hits.Select(hit =>
            {
                // hit field values are returned as JsonElement values
                if (hit.Fields?.TryGetValue(IndexConstants.FieldNames.Key, out var keyValue) is true
                    && keyValue is JsonElement { ValueKind: JsonValueKind.Array } keyJsonValue
                    && Guid.TryParse(keyJsonValue.EnumerateArray().FirstOrDefault().GetString(), out var key))
                {
                    var objectType =
                        hit.Fields?.TryGetValue(IndexConstants.FieldNames.ObjectType, out var objectTypeValue) is true
                        && objectTypeValue is JsonElement { ValueKind: JsonValueKind.Array } objectTypeJsonValue
                        && Enum.TryParse<UmbracoObjectTypes>(
                            objectTypeJsonValue.EnumerateArray().FirstOrDefault().GetString(), out var parserObjectType)
                            ? parserObjectType
                            : UmbracoObjectTypes.Unknown;

                    return new Document(key, objectType);
                }

                _logger.LogWarning("Required document fields were not found in Elastic search result hit: {hitId}", hit.Id);
                return null;
            })
            .WhereNotNull()
            .ToArray();

    private IEnumerable<FacetResult> ExtractFacetResult(Facet[] facets, AggregateDictionary? aggregations)
    {
        if (aggregations is not null)
        {
            return facets
                .Select(facet => ExtractSingleAggregateFacetResult(facet, aggregations))
                .WhereNotNull()
                .ToArray();
        }

        if (facets.Length is not 0)
        {
            _logger.LogWarning("Expected {facetCount} facet aggregations from Elastic, but got none.", facets.Length);
        }

        return [];
    }

    private FacetResult? ExtractSingleAggregateFacetResult(Facet facet, AggregateDictionary aggregations)
    {
        if (aggregations.TryGetValue(FacetName(facet), out var aggregation) is false)
        {
            _logger.LogWarning("Could not find any Elastic facet aggregation for facet: {facetName}. Facet results might be incorrect.", facet.FieldName);
            return null;
        }

        if (aggregation is FilterAggregate filterAggregate)
        {
            if(filterAggregate.Aggregations.TryGetValue(facet.FieldName, out aggregation) is false)
            {
                _logger.LogWarning("Could not find the expected, nested Elastic facet aggregation for facet: {facetName}. Facet results might be incorrect.", facet.FieldName);
                return null;
            }
        }
        
        var facetResult = facet switch
        {
            KeywordFacet keywordFacet when aggregation is StringTermsAggregate stringTermsAggregate
                => new FacetResult(
                    keywordFacet.FieldName,
                    stringTermsAggregate.Buckets.Select(bucket =>
                        new KeywordFacetValue(bucket.Key.TryGetString(out var key) ? key : "-", bucket.DocCount))),
            IntegerExactFacet integerExactFacet when aggregation is LongTermsAggregate longTermsAggregate
                => new FacetResult(
                    integerExactFacet.FieldName,
                    longTermsAggregate.Buckets.Select(bucket =>
                        new IntegerExactFacetValue(Convert.ToInt32(bucket.Key), bucket.DocCount))),
            IntegerRangeFacet integerRangeFacet when aggregation is RangeAggregate rangeAggregate
                => new FacetResult(
                    integerRangeFacet.FieldName,
                    rangeAggregate.Buckets.Select(bucket => new IntegerRangeFacetValue(
                            bucket.Key ?? "n/a",
                            bucket.From.HasValue ? Convert.ToInt32(bucket.From.Value) : null,
                            bucket.To.HasValue ? Convert.ToInt32(bucket.To.Value) : null,
                            bucket.DocCount
                        )
                    )
                ),
            DecimalExactFacet decimalExactFacet when aggregation is DoubleTermsAggregate doubleTermsAggregate
                => new FacetResult(
                    decimalExactFacet.FieldName,
                    doubleTermsAggregate.Buckets.Select(bucket =>
                        new DecimalExactFacetValue(Convert.ToDecimal(bucket.Key), bucket.DocCount))),
            DecimalRangeFacet decimalRangeFacet when aggregation is RangeAggregate rangeAggregate
                => new FacetResult(
                    decimalRangeFacet.FieldName,
                    rangeAggregate.Buckets.Select(bucket => new DecimalRangeFacetValue(
                            bucket.Key ?? "n/a",
                            bucket.From.HasValue ? Convert.ToDecimal(bucket.From.Value) : null,
                            bucket.To.HasValue ? Convert.ToDecimal(bucket.To.Value) : null,
                            bucket.DocCount
                        )
                    )
                ),
            DateTimeOffsetExactFacet dateTimeOffsetExactFacet when aggregation is LongTermsAggregate longTermsAggregate
                => new FacetResult(
                    dateTimeOffsetExactFacet.FieldName,
                    longTermsAggregate.Buckets.Select(bucket =>
                        new DateTimeOffsetExactFacetValue(DateTimeOffset.FromUnixTimeMilliseconds(bucket.Key), bucket.DocCount))),
            DateTimeOffsetRangeFacet dateTimeOffsetRangeFacet when aggregation is RangeAggregate rangeAggregate
                => new FacetResult(
                    dateTimeOffsetRangeFacet.FieldName,
                    rangeAggregate.Buckets.Select(bucket => new DateTimeOffsetRangeFacetValue(
                            bucket.Key ?? "n/a",
                            bucket.From.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(bucket.From.Value)) : null,
                            bucket.To.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(bucket.To.Value)) : null,
                            bucket.DocCount
                        )
                    )
                ),
            // this happens when querying for a facet that does not exist in the result set
            _ when aggregation is StringTermsAggregate { Buckets.Count: 0 } => null,
            _ => throw new ArgumentOutOfRangeException(nameof(facet), $"Encountered an unsupported facet type: {facet.GetType().Name}")
        };

        if (facetResult is null)
        {
            _logger.LogWarning("Unable to extract facet results for facet: {facetName}. Possible mismatch between the requested facet type and the indexed facet value. Facet results might be incorrect.", facet.FieldName);
					 
        }

        return facetResult;
    }

    private string FieldName(Filter filter)
        => filter switch
        {
            DateTimeOffsetExactFilter or DateTimeOffsetRangeFilter => FieldName(filter.FieldName, IndexConstants.FieldTypePostfix.DateTimeOffsets),
            DecimalExactFilter or DecimalRangeFilter => FieldName(filter.FieldName, IndexConstants.FieldTypePostfix.Decimals),
            IntegerExactFilter or IntegerRangeFilter => FieldName(filter.FieldName, IndexConstants.FieldTypePostfix.Integers),
            KeywordFilter => FieldName(filter.FieldName, IndexConstants.FieldTypePostfix.Keywords),
            TextFilter => FieldName(filter.FieldName, IndexConstants.FieldTypePostfix.Texts),
            _ => throw new ArgumentOutOfRangeException(nameof(filter), $"Encountered an unsupported filter type: {filter.GetType().Name}")
        };
    
    private string FieldName(Facet facet)
        => facet switch
        {
            IntegerExactFacet or IntegerRangeFacet => FieldName(facet.FieldName, IndexConstants.FieldTypePostfix.Integers),
            KeywordFacet => FieldName(facet.FieldName, IndexConstants.FieldTypePostfix.Keywords),
            DecimalExactFacet or DecimalRangeFacet => FieldName(facet.FieldName, IndexConstants.FieldTypePostfix.Decimals),
            DateTimeOffsetExactFacet or DateTimeOffsetRangeFacet => FieldName(facet.FieldName, IndexConstants.FieldTypePostfix.DateTimeOffsets),
            _ => throw new ArgumentOutOfRangeException(nameof(facet), $"Encountered an unsupported facet type: {facet.GetType().Name}")
        };

    private string FieldName(Sorter sorter)
        => sorter switch
        {
            DateTimeOffsetSorter => FieldName(sorter.FieldName, IndexConstants.FieldTypePostfix.DateTimeOffsets),
            DecimalSorter => FieldName(sorter.FieldName, IndexConstants.FieldTypePostfix.Decimals),
            IntegerSorter => FieldName(sorter.FieldName, IndexConstants.FieldTypePostfix.Integers),
            KeywordSorter => FieldName(sorter.FieldName, IndexConstants.FieldTypePostfix.Keywords),
            // NOTE: we're utilizing that dynamically mapped text fields have a .keyword subfield 
            StringSorter => FieldName(sorter.FieldName, $"{IndexConstants.FieldTypePostfix.Texts}.keyword"),
            _ => throw new ArgumentOutOfRangeException(nameof(sorter), $"Encountered an unsupported sorter type: {sorter.GetType().Name}")
        };

    private string FacetName(Facet facet)
        => $"{facet.FieldName}_{facet.GetType().Name}";

    private Action<QueryDescriptor<SearchResultDocument>> FilterDescriptor(Filter filter)
        => filter switch
        {
            TextFilter textFilter => qd => qd
                .Bool(bd => bd.Should(WildcardFilterQueryDescriptors(textFilter))),
            KeywordFilter keywordFilter => qd => qd
                .Terms(td => td
                    .Field(FieldName(filter))
                    .Terms(new TermsQueryField(keywordFilter.Values.Select(FieldValue.String).ToArray()))
                ),
            IntegerExactFilter integerExactFilter => qd => qd
                .Terms(td => td
                    .Field(FieldName(filter))
                    .Terms(new TermsQueryField(integerExactFilter.Values.Select(v => FieldValue.Long(v)).ToArray()))
                ),
            IntegerRangeFilter integerRangeFilter => qd => qd
                .Bool(bd => bd
                    .Should(integerRangeFilter
                        .Ranges
                        .Select(r => IntegerRangeFilterQueryDescriptor(FieldName(filter), r))
                        .ToArray()
                    )
                ),
            DecimalExactFilter decimalExactFilter => qd => qd
                .Terms(td => td
                    .Field(FieldName(filter))
                    .Terms(new TermsQueryField(decimalExactFilter.Values.Select(v => FieldValue.Double(Convert.ToDouble(v))).ToArray()))
                ),
            DecimalRangeFilter decimalRangeFilter => qd => qd
                .Bool(bd => bd
                    .Should(decimalRangeFilter
                        .Ranges
                        .Select(r => DecimalRangeFilterQueryDescriptor(FieldName(filter), r))
                        .ToArray()
                    )
                ),
            DateTimeOffsetExactFilter dateTimeOffsetExactFilter => qd => qd
                .Terms(td => td
                    .Field(FieldName(filter))
                    .Terms(new TermsQueryField(dateTimeOffsetExactFilter.Values.Select(v => FieldValue.String(v.ToString("O"))).ToArray()))
                ),
            DateTimeOffsetRangeFilter dateTimeOffsetRangeFilter => qd => qd
                .Bool(bd => bd
                    .Should(dateTimeOffsetRangeFilter
                        .Ranges
                        .Select(r => DateTimeOffsetRangeFilterQueryDescriptor(FieldName(filter), r))
                        .ToArray()
                    )
                ),
            _ => throw new ArgumentOutOfRangeException(nameof(filter), $"Encountered an unsupported filter type: {filter.GetType().Name}")
        };

    private Action<QueryDescriptor<SearchResultDocument>>[] WildcardFilterQueryDescriptors(TextFilter textFilter)
    {
        var fieldName = FieldName(textFilter);
        return textFilter.Values.Select(text => WildcardFilterQueryDescriptor(fieldName, text)).ToArray();
    }
    
    private Action<QueryDescriptor<SearchResultDocument>> WildcardFilterQueryDescriptor(string fieldName, string text)
        => ad => ad.Wildcard(wd => wd
            .Field(fieldName)
            .Value($"{text.Replace("*", string.Empty)}*")
        );

    private Action<QueryDescriptor<SearchResultDocument>> IntegerRangeFilterQueryDescriptor(string fieldName, FilterRange<int?> filterRange)
        => ad => ad.Range(rd => rd
            .NumberRange(nr => nr
                .Field(fieldName)
                .Gte(filterRange.MinimumValue ?? int.MinValue)
                // TODO: is this correct? verify range in/exclusion with search abstractions
                .Lt(filterRange.MaximumValue ?? int.MaxValue)
            )
        );

    private Action<QueryDescriptor<SearchResultDocument>> DecimalRangeFilterQueryDescriptor(string fieldName, FilterRange<decimal?> filterRange)
        => ad => ad.Range(rd => rd
            .NumberRange(nr => nr
                .Field(fieldName)
                .Gte(filterRange.MinimumValue.HasValue ? Convert.ToDouble(filterRange.MinimumValue.Value) : double.MinValue)
                // TODO: is this correct? verify range in/exclusion with search abstractions
                .Lt(filterRange.MaximumValue.HasValue ? Convert.ToDouble(filterRange.MaximumValue.Value) : double.MaxValue)
            )
        );

    private Action<QueryDescriptor<SearchResultDocument>> DateTimeOffsetRangeFilterQueryDescriptor(string fieldName, FilterRange<DateTimeOffset?> filterRange)
        => ad => ad.Range(rd => rd
            .Date(nr => nr
                .Field(fieldName)
                .Gte((filterRange.MinimumValue ?? DateTimeOffset.MinValue).DateTime)
                // TODO: is this correct? verify range in/exclusion with search abstractions
                .Lt((filterRange.MaximumValue ?? DateTimeOffset.MaxValue).DateTime)
            )
        );

    private Action<SortOptionsDescriptor<SearchResultDocument>> SortOptionsDescriptor(Sorter sorter)
        => sorter switch
        {
            ScoreSorter => sd => sd
                .Score(new ScoreSort
                {
                    Order = sorter.Direction is Direction.Ascending ? SortOrder.Asc : SortOrder.Desc
                }),
            _ => sd => sd
                .Field(
                    FieldName(sorter),
                    fd => fd
                        .Order(sorter.Direction is Direction.Ascending ? SortOrder.Asc : SortOrder.Desc)
                        .NumericType(
                            sorter switch
                            {
                                StringSorter or KeywordSorter => null,
                                IntegerSorter => FieldSortNumericType.Long,
                                DecimalSorter => FieldSortNumericType.Double,
                                DateTimeOffsetSorter => FieldSortNumericType.Date,
                                _ => throw new ArgumentOutOfRangeException(nameof(sorter), $"Encountered an unsupported sorter type: {sorter.GetType().Name}")
                            }
                        )) 
        };

    // NOTE: the Elastic client is strongly typed, but since we don't care about indexed data, we won't be returning any;
    //       we'll use explicit fields extraction to get the document keys from a search result.
    private record SearchResultDocument
    {
    }
}