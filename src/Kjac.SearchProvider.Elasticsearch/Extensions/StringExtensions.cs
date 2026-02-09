using Kjac.SearchProvider.Elasticsearch.Constants;

namespace Kjac.SearchProvider.Elasticsearch.Extensions;

internal static class StringExtensions
{
    public static string IndexCulture(this string? culture)
        => culture?.ToLowerInvariant() ?? IndexConstants.Variation.InvariantCulture;
}
