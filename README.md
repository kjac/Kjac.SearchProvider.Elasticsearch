# Umbraco search provider for Elasticsearch

This repo contains an alternative search provider for [Umbraco search](https://TODO), based on Elasticsearch 9.

## Prerequisites

An Elasticsearch 9 engine must be available and running 😛

## Installation

The package is installed from [NuGet](https://www.nuget.org/packages/Kjac.SearchProvider.Elasticsearch):

```
```bash
dotnet add package Kjac.SearchProvider.Elasticsearch
```

Once installed, add the search provider to Umbraco by means of composition:

```csharp
using Kjac.SearchProvider.Elasticsearch.DependencyInjection;
using Umbraco.Cms.Core.Composing;

namespace My.Site;

public class SiteComposer : IComposer
{
    public void Compose(IUmbracoBuilder builder)
        => builder.AddElasticsearchSearchProvider();
}
```

## Connecting to the Elasticsearch engine

You'll need to configure the search provider, so it can connect to your Elasticsearch engine.

This is done either via `appsettings.json`:

```json
{
  "ElasticsearchSearchProvider": {
    "Client": {
      "Host": "[your Elasticsearch host]",
      "Authentication": {
        "ApiKey": "[your API key]"
      },
      "EnableDebugMode": [true/false]
    }
  }
}
```

...or using `IOptions`:

```csharp
using Kjac.SearchProvider.Elasticsearch.Configuration;
using Kjac.SearchProvider.Elasticsearch.DependencyInjection;
using Umbraco.Cms.Core.Composing;

namespace My.Site;

public class SiteComposer : IComposer
{
    public void Compose(IUmbracoBuilder builder)
    {
        builder.AddElasticsearchSearchProvider();

        builder.Services.Configure<ClientOptions>(options =>
        {
            options.Host = new("http://localhost:9200");
            options.Authentication = new()
            {
                ApiKey = "my-api-key"
            };
            options.EnableDebugMode = true;
        });
    }
}
```

> [!NOTE]
> The `EnableDebugMode` enables a _very_ verbose console logging from Elasticsearch. It's useful both for troubleshooting connectivity errors and general debugging.

### Basic authentication

While the API keys are encouraged by Elasticsearch, it is also possible to use basic authentication, provided of course that your engine supports that:

```json
{
  "ElasticsearchSearchProvider": {
    "Client": {
      "Authentication": {
        "Username": "[your user name]",
        "Password": "[your password]"
      }
    }
  }
}
```

...or:

```csharp
builder.Services.Configure<ClientOptions>(options =>
{
    options.Authentication = new()
    {
        Username = "[your user name]",
        Password = "[your password]"
    };
});
```

## Extendability

Generally, you should look to Umbraco search for extension points. There are however a few notable extension points in this search provider as well.

### Tweaking score boosting for textual relevance

TODO: VERIFY DEFAULTS

Umbraco search allows for multiple textual relevance options within a single field. You can change the boost factors of the search provider by configuring the `SearcherOptions`:

```csharp
builder.Services.Configure<SearcherOptions>(options =>
{
    // boost the highest relevance text by a factor 100 (default is 6)
    options.BoostFactorTextR1 = 100f;
    // boost the second-highest relevance text by a factor 10 (default is 4)
    options.BoostFactorTextR2 = 10f;
    // do not boost the third-highest relevance text at all (default is 2)
    options.BoostFactorTextR1 = 1f;
});
```

### Allowing for more facet values

TODO: VERIFY DEFAULTS

By default, the search provider allows for a maximum of 100 facet values returned per facet in a search result. You can change that - also using `SearcherOptions`:

```csharp
builder.Services.Configure<SearcherOptions>(options =>
{
    // allow fetching 200 facet values per facet
    options.MaxFacetValues = 200;
});
```

> [!IMPORTANT]
> Increasing the maximum number of facet values per facet can degrade your overall search performance. Use with caution.

### Client connectivity

TODO: WORDS HERE

[`IElasticsearchClientFactory`](https://github.com/kjac/Kjac.SearchProvider.Elasticsearch/blob/main/src/Kjac.SearchProvider.Elasticsearch/Services/IElasticsearchClientFactory.cs)

### Index management

TODO: WORDS HERE

[`IElasticsearchIndexManager`](https://github.com/kjac/Kjac.SearchProvider.Elasticsearch/blob/main/src/Kjac.SearchProvider.Elasticsearch/Services/IElasticsearchIndexManager.cs)

## Contributing

Yes, please ❤️

When raising an issue, please make sure to include plenty of context and steps to reproduce in the issue description. That saves me from asking you for it 😉 

If you're submitting a PR, please:

1. Also include plenty of context and steps to reproduce.
2. Make sure your code follows the provided editor configuration.
3. If at all possible, create tests that prove the issue has been fixed.
   - You'll find instructions on running the tests [here](https://github.com/kjac/Kjac.SearchProvider.Elasticsearch/tree/main/src/Kjac.SearchProvider.Elasticsearch.Tests). 
