## Example Druid Extension

This is an example project demonstrating how to write a Druid extension. It includes:

- ExampleExtractionFn, an extraction function.
- ExampleParseSpec, a parser.
- ExampleExtensionModule, the class that registers these with Druid's extension system.
- META-INF/services/io.druid.initialization.DruidModule entry for ExampleExtensionModule.

It also includes unit tests for the above.

You can extend Druid with custom aggregators, query types, filters, and many more as well.

### Build

To build the extension, run `mvn package` and you'll get a file in `target` like this:

```
[INFO] Building tar: /src/druid-example-extension/target/druid-example-extension-0.9.1.1_1-SNAPSHOT-bin.tar.gz
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 4.841 s
[INFO] Finished at: 2045-11-04T20:00:53Z
[INFO] Final Memory: 21M/402M
[INFO] ------------------------------------------------------------------------
```

Unpack the tar.gz and you'll find a directory named `druid-example-extension` inside it:

```
$ tar xzf target/druid-example-extension-0.9.1.1_1-SNAPSHOT-bin.tar.gz
$ ls druid-example-extension-0.9.1.1_1-SNAPSHOT/
LICENSE                  README.md                druid-example-extension/
```

### Install

To install the extension:

1. Copy `druid-example-extension` into your Druid `extensions` directory.
2. Edit `conf/_common/common.runtime.properties` to add `"druid-example-extension"` to `druid.extensions.loadList`.
It should look like: `druid.extensions.loadList=["druid-example-extension"]`. There may be a few other extensions there
too.
3. Restart Druid.

### Use

To use the extractionFn, call it like a normal extractionFn with type "example", e.g. in a topN:

```json
{
  "queryType": "topN",
  "dataSource": "wikiticker",
  "intervals": [
    "2016-06-27/2016-06-28"
  ],
  "granularity": "all",
  "dimension": {
    "type": "extraction",
    "dimension": "page",
    "outputName": "page",
    "extractionFn": {
      "type": "example",
      "length": 5
    }
  },
  "metric": "edits",
  "threshold": 25,
  "aggregations": [
    {
      "type": "longSum",
      "name": "edits",
      "fieldName": "count"
    }
  ]
}
```

To use the parser, configure it like a normal parser with format "example", e.g. in an indexing spec:

```json
"parser" : {
  "type": "string",
  "parseSpec": {
    "format": "example",
    "extractionFn": {
      "type": "javascript",
      "function": "function(x) { if (x == null ) { return x; } else { return x.split('').reverse().join(''); } }"
    },
    "dimensionsSpec": {
      "dimensions": [
        "channel",
        "cityName",
        "comment"
      ]
    },
    "timestampSpec": {
      "format": "auto",
      "column": "timestamp"
    }
  }
}
```
