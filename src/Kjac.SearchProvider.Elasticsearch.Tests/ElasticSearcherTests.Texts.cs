using Umbraco.Cms.Core;
using Umbraco.Cms.Search.Core.Models.Searching.Filtering;
using Umbraco.Cms.Search.Core.Models.Searching.Sorting;

namespace Kjac.SearchProvider.Elasticsearch.Tests;

// tests specifically related to the IndexValue.Texts collection
public partial class ElasticSearcherTests
{
    [Test]
    public async Task CanFilterSingleDocumentBySpecificText()
    {
        var result = await SearchAsync(
            filters: [new TextFilter(FieldMultipleValues, ["single12"], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(1));
            Assert.That(result.Documents.First().Id, Is.EqualTo(_documentIds[12]));
        });
    }

    [Test]
    public async Task CanFilterMultipleDocumentsBySpecificText()
    {
        var result = await SearchAsync(
            filters: [new TextFilter(FieldMultipleValues, ["single11", "single22", "single33"], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(3));

            var documents = result.Documents.ToList();
            Assert.That(
                documents.Select(d => d.Id),
                Is.EqualTo(new[]
                {
                    _documentIds[11],
                    _documentIds[22],
                    _documentIds[33]
                }).AsCollection
            );
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanFilterMultipleDocumentsByCommonText(bool even)
    {
        var result = await SearchAsync(
            filters: [new TextFilter(FieldMultipleValues, [even ? "even" : "odd"], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(50));

            var documents = result.Documents.ToList();
            var expectedIds = OddOrEvenIds(even);
            Assert.That(
                documents.Select(d => d.Id),
                Is.EqualTo(expectedIds.Select(id => _documentIds[id])).AsCollection
            );
        });
    }

    [Test]
    public async Task CanFilterDocumentsBySpecificTextNegated()
    {
        var result = await SearchAsync(
            filters: [new TextFilter(FieldMultipleValues, ["single12"], true)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(99));
            Assert.That(result.Documents.Select(d => d.Id), Is.EqualTo(_documentIds.Values.Except([_documentIds[12]])).AsCollection);
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanFilterDocumentsByCommonTextNegated(bool even)
    {
        var result = await SearchAsync(
            filters: [new TextFilter(FieldMultipleValues, [even ? "even" : "odd"], true)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(50));

            var documents = result.Documents.ToList();
            var expectedIds = OddOrEvenIds(even is false);
            Assert.That(
                documents.Select(d => d.Id),
                Is.EqualTo(expectedIds.Select(id => _documentIds[id])).AsCollection
            );
        });
    }

    [Test]
    public async Task CanFilterAllDocumentsByWildcardText()
    {
        var result = await SearchAsync(
            filters: [new TextFilter(FieldMultipleValues, ["single"], false)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(100));
            Assert.That(result.Documents.Select(d => d.Id), Is.EqualTo(_documentIds.Values).AsCollection);
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanSortDocumentsByText(bool ascending)
    {
        var result = await SearchAsync(
            sorters: [new StringSorter(FieldSingleValue, ascending ? Direction.Ascending : Direction.Descending)]
        );

        Assert.Multiple(() =>
        {
            Assert.That(result.Total, Is.EqualTo(100));
            Assert.That(result.Documents.First().Id, Is.EqualTo(ascending ? _documentIds[1] : _documentIds[99]));
        });
    }
}