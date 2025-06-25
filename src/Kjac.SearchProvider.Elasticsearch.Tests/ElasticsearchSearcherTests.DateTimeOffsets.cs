﻿using Umbraco.Cms.Core;
using Umbraco.Cms.Search.Core.Models.Searching.Faceting;
using Umbraco.Cms.Search.Core.Models.Searching.Filtering;
using Umbraco.Cms.Search.Core.Models.Searching.Sorting;
using Umbraco.Extensions;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

// tests specifically related to the IndexValue.DateTimeOffsets collection
public partial class ElasticsearchSearcherTests
{
    [Test]
    public async Task CanFilterSingleDocumentByDateTimeOffsetExact()
    {
        var result = await SearchAsync(
            filters: [new DateTimeOffsetExactFilter(FieldMultipleValues, [StartDate().AddDays(1)], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(1));
            Assert.That(result.Documents.First().Id, Is.EqualTo(_documentIds[1]));
        });
    }

    [Test]
    public async Task CanFilterSingleDocumentByDateTimeOffsetRange()
    {
        var result = await SearchAsync(
            filters: [new DateTimeOffsetRangeFilter(FieldMultipleValues, [new FilterRange<DateTimeOffset?>(StartDate().AddDays(1), StartDate().AddDays(2))], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(1));
            Assert.That(result.Documents.First().Id, Is.EqualTo(_documentIds[1]));
        });
    }

    [Test]
    public async Task CanFilterMultipleDocumentsByDateTimeOffsetExact()
    {
        var result = await SearchAsync(
            filters: [new DateTimeOffsetExactFilter(FieldMultipleValues, [StartDate().AddDays(10), StartDate().AddDays(50), StartDate().AddDays(100)], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(5));

            var documents = result.Documents.ToList();
            // expecting 5 (10), 10 (10), 25 (50), 50 (50 + 100) and 100 (100)
            Assert.That(
                documents.Select(d => d.Id),
                Is.EqualTo(new[]
                {
                    _documentIds[5],
                    _documentIds[10],
                    _documentIds[25],
                    _documentIds[50],
                    _documentIds[100]
                }).AsCollection
            );
        });
    }

    [Test]
    public async Task CanFilterMultipleDocumentsByDateTimeOffsetRange()
    {
        var result = await SearchAsync(
            filters: [
                new DateTimeOffsetRangeFilter(
                    FieldMultipleValues,
                    [
                        new FilterRange<DateTimeOffset?>(StartDate().AddDays(1), StartDate().AddDays(5)),
                        new FilterRange<DateTimeOffset?>(StartDate().AddDays(20), StartDate().AddDays(25)),
                        new FilterRange<DateTimeOffset?>(StartDate().AddDays(100), StartDate().AddDays(101))
                    ],
                    false
                )
            ]
        );

        Assert.Multiple(() =>
        {
            // expecting
            // - first range: 1, 2, 3, 4
            // - second range: 10 (20), 11 (22), 12 (24), 20, 21, 22, 23, 24
            // - third range: 50 (100), 100
            Assert.That(result.Total, Is.EqualTo(14));

            var documents = result.Documents.ToList();
            Assert.That(
                documents.Select(d => d.Id),
                Is.EquivalentTo(new[]
                {
                    _documentIds[1],
                    _documentIds[2],
                    _documentIds[3],
                    _documentIds[4],
                    _documentIds[10],
                    _documentIds[11],
                    _documentIds[12],
                    _documentIds[20],
                    _documentIds[21],
                    _documentIds[22],
                    _documentIds[23],
                    _documentIds[24],
                    _documentIds[50],
                    _documentIds[100],
                })
            );
        });
    }

    [Test]
    public async Task CanFilterDocumentsByDateTimeOffsetExactNegated()
    {
        var result = await SearchAsync(
            filters: [new DateTimeOffsetExactFilter(FieldMultipleValues, [StartDate().AddDays(1)], true)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(99));
            Assert.That(result.Documents.Select(d => d.Id), Is.EqualTo(_documentIds.Values.Skip(1)).AsCollection);
        });
    }

    [Test]
    public async Task CanFilterDocumentsByDateTimeOffsetRangeNegated()
    {
        var result = await SearchAsync(
            filters: [new DateTimeOffsetRangeFilter(FieldMultipleValues, [new FilterRange<DateTimeOffset?>(StartDate().AddDays(1), StartDate().AddDays(2))], true)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(99));
            Assert.That(result.Documents.Select(d => d.Id), Is.EqualTo(_documentIds.Values.Skip(1)).AsCollection);
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanFacetDocumentsByDateTimeOffsetExact(bool filtered)
    {
        var result = await SearchAsync(
            facets: [new DateTimeOffsetExactFacet(FieldMultipleValues)],
            filters: filtered ? [new DateTimeOffsetExactFilter(FieldMultipleValues, [StartDate().AddDays(1), StartDate().AddDays(2), StartDate().AddDays(3)], false)] : []
        );

        // expecting the same facets whether filtering is enabled or not, because
        // both faceting and filtering is applied to the same field
        var expectedFacetValues = Enumerable
            .Range(1, 100)
            .SelectMany(i => new[] { 0, i, i * 2 }.Select(i2 => StartDate().AddDays(i2)))
            .GroupBy(i => i)
            .Select(group => new
            {
                Key = group.Key,
                Count = group.Count()
            })
            .ToArray();

        // expecting
        // - when filtered: 1, 2 and 3
        // - when not filtered: all of them
        Assert.That(result.Total, Is.EqualTo(filtered ? 3 : 100));

        var facets = result.Facets.ToArray();
        Assert.That(facets, Has.Length.EqualTo(1));

        var facet = facets.First();
        Assert.That(facet.FieldName, Is.EqualTo(FieldMultipleValues));

        var facetValues = facet.Values.OfType<DateTimeOffsetExactFacetValue>().ToArray();
        Assert.That(facetValues, Has.Length.EqualTo(expectedFacetValues.Length));
        foreach (var expectedFacetValue in expectedFacetValues)
        {
            var facetValue = facetValues.FirstOrDefault(f => f.Key == expectedFacetValue.Key); 
            Assert.That(facetValue, Is.Not.Null);
            Assert.That(facetValue.Count, Is.EqualTo(expectedFacetValue.Count));
        }
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanFacetDocumentsByDateTimeOffsetRange(bool filtered)
    {
        var result = await SearchAsync(
            facets: [
                new DateTimeOffsetRangeFacet(
                    FieldMultipleValues,
                    [
                        new DateTimeOffsetRangeFacetRange("One", StartDate().AddDays(1), StartDate().AddDays(25)),
                        new DateTimeOffsetRangeFacetRange("Two", StartDate().AddDays(25), StartDate().AddDays(50)),
                        new DateTimeOffsetRangeFacetRange("Three", StartDate().AddDays(50), StartDate().AddDays(75)),
                        new DateTimeOffsetRangeFacetRange("Four", StartDate().AddDays(75), StartDate().AddDays(100))
                    ]
                )
            ],
            filters: filtered ? [new DateTimeOffsetExactFilter(FieldMultipleValues, [StartDate().AddDays(1), StartDate().AddDays(2), StartDate().AddDays(3)], false)] : []
        );

        // expecting the same facets whether filtering is enabled or not, because
        // both faceting and filtering is applied to the same field
        var expectedFacetValues = Enumerable
            .Range(1, 100)
            .SelectMany(i => new[] { i, i * 2 }
                .Select(value => value switch
                {
                    < 25 => "One",
                    < 50 => "Two",
                    < 75 => "Three",
                    < 100 => "Four",
                    _ => null
                })
                .WhereNotNull()
                .Distinct()
            )
            .GroupBy(key => key)
            .Select(group => new
            {
                Key = group.Key,
                Count = group.Count()
            })
            .WhereNotNull()
            .ToArray();

        // expecting
        // - when filtered: 1, 2 and 3
        // - when not filtered: all of them
        Assert.That(result.Total, Is.EqualTo(filtered ? 3 : 100));

        var facets = result.Facets.ToArray();
        Assert.That(facets, Has.Length.EqualTo(1));

        var facet = facets.First();
        Assert.That(facet.FieldName, Is.EqualTo(FieldMultipleValues));
        
        var facetValues = facet.Values.OfType<DateTimeOffsetRangeFacetValue>().ToArray();
        Assert.That(facetValues, Has.Length.EqualTo(expectedFacetValues.Length));
        foreach (var expectedFacetValue in expectedFacetValues)
        {
            var facetValue = facetValues.FirstOrDefault(f => f.Key == expectedFacetValue.Key); 
            Assert.That(facetValue, Is.Not.Null);
            Assert.That(facetValue.Count, Is.EqualTo(expectedFacetValue.Count));
        }
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanSortDocumentsByDateTimeOffset(bool ascending)
    {
        var result = await SearchAsync(
            sorters: [new DateTimeOffsetSorter(FieldSingleValue, ascending ? Direction.Ascending : Direction.Descending)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(100));
            Assert.That(result.Documents.First().Id, Is.EqualTo(ascending ? _documentIds[1] : _documentIds[100]));
        });
    }
}