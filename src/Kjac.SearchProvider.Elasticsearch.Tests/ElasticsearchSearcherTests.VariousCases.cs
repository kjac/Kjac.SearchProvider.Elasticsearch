using Umbraco.Cms.Core;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Search.Core.Models.Searching.Faceting;
using Umbraco.Cms.Search.Core.Models.Searching.Filtering;
using Umbraco.Cms.Search.Core.Models.Searching.Sorting;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

// various tests unrelated to specific IndexValue collections or spanning multiple IndexValue collections
public partial class ElasticsearchSearcherTests
{
    [Test]
    public async Task FilteringWithoutFacetsYieldsNoFacetValues()
    {
        var result = await SearchAsync(
            filters: [new IntegerExactFilter(FieldSingleValue, [1, 2, 3], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(3));
            Assert.That(result.Facets, Is.Empty);
        });
    }

    [Test]
    public async Task CanRetrieveObjectTypes()
    {
        var result = await SearchAsync(
            filters: [new IntegerExactFilter(FieldSingleValue, [1, 26, 51, 76], false)]
        );

        Assert.That(result.Total, Is.EqualTo(4));

        Assert.Multiple(() =>
        {
            var documents = result.Documents.ToArray();
            Assert.That(documents[0].ObjectType, Is.EqualTo(UmbracoObjectTypes.Document));
            Assert.That(documents[1].ObjectType, Is.EqualTo(UmbracoObjectTypes.Media));
            Assert.That(documents[2].ObjectType, Is.EqualTo(UmbracoObjectTypes.Member));
            Assert.That(documents[3].ObjectType, Is.EqualTo(UmbracoObjectTypes.Unknown));
        });
    }

    [Test]
    public async Task CanCombineFacetsWithinFields()
    {
        var result = await SearchAsync(
            facets: [
                new IntegerExactFacet(FieldSingleValue),
                new KeywordFacet(FieldSingleValue)
            ]
        );

        Assert.That(result.Total, Is.EqualTo(100));

        var facets = result.Facets.ToArray();
        Assert.That(facets, Has.Length.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(facets[0].FieldName, Is.EqualTo(FieldSingleValue));
            Assert.That(facets[1].FieldName, Is.EqualTo(FieldSingleValue));
        });

        var integerFacetValues = facets[0].Values.OfType<IntegerExactFacetValue>().ToArray();
        var keywordFacetValues = facets[1].Values.OfType<KeywordFacetValue>().ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(integerFacetValues, Has.Length.EqualTo(100));
            Assert.That(keywordFacetValues, Has.Length.EqualTo(100));
        });

        for (var i = 0; i < 100; i++)
        {
            Assert.Multiple(() =>
            {
                Assert.That(integerFacetValues[i].Key, Is.EqualTo(i + 1));
                Assert.That(integerFacetValues[i].Count, Is.EqualTo(1));

                var keywordFacetValue = keywordFacetValues.FirstOrDefault(v => v.Key == $"single{i + 1}");
                Assert.That(keywordFacetValue?.Count, Is.EqualTo(1));
            });
        }
    }

    [Test]
    public async Task CanCombineFacetsAcrossFields()
    {
        var result = await SearchAsync(
            facets: [
                new IntegerExactFacet(FieldSingleValue),
                new KeywordFacet(FieldMultipleValues)
            ]
        );

        Assert.That(result.Total, Is.EqualTo(100));

        var facets = result.Facets.ToArray();
        Assert.That(facets, Has.Length.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(facets[0].FieldName, Is.EqualTo(FieldSingleValue));
            Assert.That(facets[1].FieldName, Is.EqualTo(FieldMultipleValues));
        });

        var integerFacetValues = facets[0].Values.OfType<IntegerExactFacetValue>().ToArray();
        var keywordFacetValues = facets[1].Values.OfType<KeywordFacetValue>().ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(integerFacetValues, Has.Length.EqualTo(100));
            Assert.That(keywordFacetValues, Has.Length.EqualTo(103));
        });

        Assert.Multiple(() =>
        {
            Assert.That(keywordFacetValues.FirstOrDefault(v => v.Key == "all")?.Count, Is.EqualTo(100));
            Assert.That(keywordFacetValues.FirstOrDefault(v => v.Key == "odd")?.Count, Is.EqualTo(50));
            Assert.That(keywordFacetValues.FirstOrDefault(v => v.Key == "even")?.Count, Is.EqualTo(50));
        });
        
        for (var i = 0; i < 100; i++)
        {
            Assert.Multiple(() =>
            {
                Assert.That(integerFacetValues[i].Key, Is.EqualTo(i + 1));
                Assert.That(integerFacetValues[i].Count, Is.EqualTo(1));

                var keywordFacetValue = keywordFacetValues.FirstOrDefault(v => v.Key == $"single{i + 1}");
                Assert.That(keywordFacetValue?.Count, Is.EqualTo(1));
            });
        }
    }
    
    [Test]
    public async Task FilteringOneFieldLimitsFacetCountForAnotherField()
    {
        var result = await SearchAsync(
            filters: [new IntegerExactFilter(FieldSingleValue, [1, 10, 25, 50, 100], false)],
            facets: [new IntegerExactFacet(FieldMultipleValues)]
        );

        Assert.That(result.Total, Is.EqualTo(5));

        var facets = result.Facets.ToArray();
        Assert.That(facets, Has.Length.EqualTo(1));

        var expectedFacets = new[]
        {
            new { Key = 1000, Count = 1 }, // 100
            new { Key = 500, Count = 1 },  // 50
            new { Key = 100, Count = 2 },  // 10, 100
            new { Key = 100, Count = 2 },  // 10, 100
            new { Key = 50, Count = 1 },   // 50
            new { Key = 25, Count = 1 },   // 25
            new { Key = 10, Count = 2 },   // 1, 10
            new { Key = 1, Count = 1 },    // 1
        };

        var facetValues = facets[0].Values.OfType<IntegerExactFacetValue>().ToArray();
        foreach (var expectedFacet in expectedFacets)
        {
            Assert.Multiple(() =>
            {
                // the integer values are mirrored around 0 (negative and positive values)
                Assert.That(facetValues.SingleOrDefault(v => v.Key == expectedFacet.Key)?.Count, Is.EqualTo(expectedFacet.Count));
                Assert.That(facetValues.SingleOrDefault(v => v.Key == -1 * expectedFacet.Key)?.Count, Is.EqualTo(expectedFacet.Count));
            });
        }
    }
    
    [Test]
    public async Task CanMixRegularAndNegatedFilters()
    {
        var result = await SearchAsync(
            filters: [
                new IntegerExactFilter(FieldSingleValue, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], false),
                new DecimalExactFilter(FieldSingleValue, [1m, 2m, 3m, 4m, 5m], true)
            ]
        );

        Assert.That(result.Total, Is.EqualTo(5));

        Assert.Multiple(() =>
        {
            // expecting 6, 7, 8, 9 and 10
            var documents = result.Documents.ToArray();
            Assert.That(documents[0].Id, Is.EqualTo(_documentIds[6]));
            Assert.That(documents[1].Id, Is.EqualTo(_documentIds[7]));
            Assert.That(documents[2].Id, Is.EqualTo(_documentIds[8]));
            Assert.That(documents[3].Id, Is.EqualTo(_documentIds[9]));
            Assert.That(documents[4].Id, Is.EqualTo(_documentIds[10]));
        });
    }
    
    [Test]
    public async Task CanMixFiltersAcrossFields()
    {
        var result = await SearchAsync(
            filters: [
                new IntegerExactFilter(FieldSingleValue, [1, 2, 3, 4, 5, 6], false),
                new IntegerExactFilter(FieldMultipleValues, [30, 50, 70, 100], false)
            ]
        );

        Assert.That(result.Total, Is.EqualTo(2));

        Assert.Multiple(() =>
        {
            // expecting 3 (30) and 5 (50) 
            var documents = result.Documents.ToArray();
            Assert.That(documents[0].Id, Is.EqualTo(_documentIds[3]));
            Assert.That(documents[1].Id, Is.EqualTo(_documentIds[5]));
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanSortOnMultipleFields(bool ascending)
    {
        var result = await SearchAsync(
            sorters: [
                new KeywordSorter(FieldMultiSorting, ascending ? Direction.Ascending : Direction.Descending),
                // NOTE: to spice things up, the integer sort order is reversed (i.e. descending when the test case is ascending)
                new IntegerSorter(FieldSingleValue, ascending ? Direction.Descending : Direction.Ascending) 
            ]
        );

        Assert.That(result.Total, Is.EqualTo(100));

        // expected: all documents sorted by "even"/"odd", subsequently by integer value reversed
        var expectedSortOrder = OddOrEvenIds(true).Reverse().Union(OddOrEvenIds(false).Reverse()).ToArray();
        if (ascending is false)
        {
            expectedSortOrder = expectedSortOrder.Reverse().ToArray();
        }

        var documents = result.Documents.ToArray();
        for (var i = 0; i < 100; i++)
        {
            Assert.That(documents[i].Id, Is.EqualTo(_documentIds[expectedSortOrder[i]]));
        }
    }
}