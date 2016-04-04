# ElasticsearchSink2

This is Flume-NG Sink for Elasticsearch => 2.0.
I developed this because the official version does not support Elasticsearch => 2.0 due to API changes.
Hope Flume dev team will fix this soon.


# Requirements

- Flume-NG => 1.6
- Elasticsearch => 2.0

# Build

Build standard jar by the following command

```bash
$ ./gradlew build
```

Build fat jar which contains elasticsearch dependencies
```bash
$ ./gradlew assembly
```

Jar will be generated in `build/libs`


# Usage

1. Append the built jar to Flume's classpath
2. remove `guava-*.jar` and `jackson-core-*.jar` in flume's default libs dir. They are outdated and newer version are included in Elasticsearch.
3. set sink type to `com.frontier45.flume.sink.elasticsearch2.ElasticSearchSink` in flume.conf
4. start flume agent
