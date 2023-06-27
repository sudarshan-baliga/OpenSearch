# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))
- Add support for ignoring missing Javadoc on generated code using annotation ([#7604](https://github.com/opensearch-project/OpenSearch/pull/7604))
- Add PSA transport action for bulk async fetch of shards ([#5098](https://github.com/opensearch-project/OpenSearch/issues/5098))

### Dependencies
- Bump `log4j-core` from 2.18.0 to 2.19.0
- Bump `forbiddenapis` from 3.3 to 3.4
- Bump `avro` from 1.11.0 to 1.11.1
- Bump `woodstox-core` from 6.3.0 to 6.3.1
- Bump `xmlbeans` from 5.1.0 to 5.1.1 ([#4354](https://github.com/opensearch-project/OpenSearch/pull/4354))
- Bump `reactor-netty-core` from 1.0.19 to 1.0.22 ([#4447](https://github.com/opensearch-project/OpenSearch/pull/4447))
- Bump `reactive-streams` from 1.0.3 to 1.0.4 ([#4488](https://github.com/opensearch-project/OpenSearch/pull/4488))
- Bump `jempbox` from 1.8.16 to 1.8.17 ([#4550](https://github.com/opensearch-project/OpenSearch/pull/4550))
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))
- Update Apache Lucene to 9.5.0-snapshot-d5cef1c ([#5570](https://github.com/opensearch-project/OpenSearch/pull/5570))
- Bump `maven-model` from 3.6.2 to 3.8.6 ([#5599](https://github.com/opensearch-project/OpenSearch/pull/5599))
- Bump `maxmind-db` from 2.1.0 to 3.0.0 ([#5601](https://github.com/opensearch-project/OpenSearch/pull/5601))
- Bump `wiremock-jre8-standalone` from 2.33.2 to 2.35.0
- Bump `gson` from 2.10 to 2.10.1
- Bump `com.google.code.gson:gson` from 2.10 to 2.10.1
- Bump `com.maxmind.geoip2:geoip2` from 4.0.0 to 4.0.1
- Bump `com.avast.gradle:gradle-docker-compose-plugin` from 0.16.11 to 0.16.12
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0
- Bump `org.apache.commons:commons-configuration2` from 2.8.0 to 2.9.0
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 19.2.0 to 20.3.0
- Bump `io.opencensus:opencensus-api` from 0.18.0 to 0.31.1 ([#7291](https://github.com/opensearch-project/OpenSearch/pull/7291))
- OpenJDK Update (April 2023 Patch releases) ([#7344](https://github.com/opensearch-project/OpenSearch/pull/7344)
- Bump `com.google.http-client:google-http-client:1.43.2` from 1.42.0 to 1.43.2 ([7928](https://github.com/opensearch-project/OpenSearch/pull/7928)))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Change http code for DecommissioningFailedException from 500 to 400 ([#5283](https://github.com/opensearch-project/OpenSearch/pull/5283))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Pass localNode info to all plugins on node start ([#7919](https://github.com/opensearch-project/OpenSearch/pull/7919))
- Modified the existing async shard fetch transport action to use the helper functions added for bulk fetching ([#5098](https://github.com/opensearch-project/OpenSearch/issues/5098))
### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))
- Remove LegacyESVersion.V_7_0_* and V_7_1_* Constants ([#2768](https://https://github.com/opensearch-project/OpenSearch/pull/2768))
- Remove LegacyESVersion.V_7_2_ and V_7_3_ Constants ([#4702](https://github.com/opensearch-project/OpenSearch/pull/4702))
- Always auto release the flood stage block ([#4703](https://github.com/opensearch-project/OpenSearch/pull/4703))
- Remove LegacyESVersion.V_7_4_ and V_7_5_ Constants ([#4704](https://github.com/opensearch-project/OpenSearch/pull/4704))
- Remove Legacy Version support from Snapshot/Restore Service ([#4728](https://github.com/opensearch-project/OpenSearch/pull/4728))
- Remove deprecated serialization logic from pipeline aggs ([#4847](https://github.com/opensearch-project/OpenSearch/pull/4847))
- Remove unused private methods ([#4926](https://github.com/opensearch-project/OpenSearch/pull/4926))
- Remove LegacyESVersion.V_7_8_ and V_7_9_ Constants ([#4855](https://github.com/opensearch-project/OpenSearch/pull/4855))
- Remove LegacyESVersion.V_7_6_ and V_7_7_ Constants ([#4837](https://github.com/opensearch-project/OpenSearch/pull/4837))
- Remove LegacyESVersion.V_7_10_ Constants ([#5018](https://github.com/opensearch-project/OpenSearch/pull/5018))
- Remove Version.V_1_ Constants ([#5021](https://github.com/opensearch-project/OpenSearch/pull/5021))
- Remove custom Map, List and Set collection classes ([#6871](https://github.com/opensearch-project/OpenSearch/pull/6871))

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fix compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Support OpenSSL Provider with default Netty allocator ([#5460](https://github.com/opensearch-project/OpenSearch/pull/5460))
- Replaces ZipInputStream with ZipFile to fix Zip Slip vulnerability ([#7230](https://github.com/opensearch-project/OpenSearch/pull/7230))
- Add missing validation/parsing of SearchBackpressureMode of SearchBackpressureSettings ([#7541](https://github.com/opensearch-project/OpenSearch/pull/7541))

### Security

## [Unreleased 2.x]
### Added
- Add task cancellation monitoring service ([#7642](https://github.com/opensearch-project/OpenSearch/pull/7642))
- Add TokenManager Interface ([#7452](https://github.com/opensearch-project/OpenSearch/pull/7452))
- Add Remote store as a segment replication source ([#7653](https://github.com/opensearch-project/OpenSearch/pull/7653))
- Implement concurrent aggregations support without profile option ([#7514](https://github.com/opensearch-project/OpenSearch/pull/7514))
- Add dynamic index and cluster setting for concurrent segment search ([#7956](https://github.com/opensearch-project/OpenSearch/pull/7956))
- Add descending order search optimization through reverse segment read. ([#7967](https://github.com/opensearch-project/OpenSearch/pull/7967))
- Update components of segrep backpressure to support remote store. ([#8020](https://github.com/opensearch-project/OpenSearch/pull/8020))
- Make remote cluster connection setup in async ([#8038](https://github.com/opensearch-project/OpenSearch/pull/8038))
- Add API to initialize extensions ([#8029]()https://github.com/opensearch-project/OpenSearch/pull/8029)

### Dependencies
- Bump `com.azure:azure-storage-common` from 12.21.0 to 12.21.1 (#7566, #7814)
- Bump `com.google.guava:guava` from 30.1.1-jre to 32.0.0-jre (#7565, #7811, #7807, #7808)
- Bump `net.minidev:json-smart` from 2.4.10 to 2.4.11 (#7660, #7812)
- Bump `org.gradle.test-retry` from 1.5.2 to 1.5.3 (#7810)
- Bump `com.diffplug.spotless` from 6.17.0 to 6.18.0 (#7896)
- Bump `jackson` from 2.15.1 to 2.15.2 ([#7897](https://github.com/opensearch-project/OpenSearch/pull/7897))
- Add `com.github.luben:zstd-jni` version 1.5.5-3 ([#2996](https://github.com/opensearch-project/OpenSearch/pull/2996))
- Bump `netty` from 4.1.91.Final to 4.1.93.Final ([#7901](https://github.com/opensearch-project/OpenSearch/pull/7901))
- Bump `com.amazonaws` 1.12.270 to `software.amazon.awssdk` 2.20.55 ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Add `org.reactivestreams` 1.0.4 ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Bump `com.networknt:json-schema-validator` from 1.0.81 to 1.0.85 ([7968], #8255)
- Bump `com.netflix.nebula:gradle-extra-configurations-plugin` from 9.0.0 to 10.0.0 in /buildSrc ([#7068](https://github.com/opensearch-project/OpenSearch/pull/7068))
- Bump `com.google.guava:guava` from 32.0.0-jre to 32.0.1-jre (#8009)
- Bump `commons-io:commons-io` from 2.12.0 to 2.13.0 (#8014, #8013, #8010)
- Bump `com.diffplug.spotless` from 6.18.0 to 6.19.0 (#8007)
- Bump `'com.azure:azure-storage-blob` to 12.22.2 from 12.21.1 ([#8043](https://github.com/opensearch-project/OpenSearch/pull/8043))
- Bump `org.jruby.joni:joni` from 2.1.48 to 2.2.1 (#8015, #8254)
- Bump `com.google.guava:guava` from 32.0.0-jre to 32.0.1-jre ([#8011](https://github.com/opensearch-project/OpenSearch/pull/8011), [#8012](https://github.com/opensearch-project/OpenSearch/pull/8012), [#8107](https://github.com/opensearch-project/OpenSearch/pull/8107))
- Bump `io.projectreactor:reactor-core` from 3.4.18 to 3.5.6 in /plugins/repository-azure ([#8016](https://github.com/opensearch-project/OpenSearch/pull/8016))
- Bump `spock-core` from 2.1-groovy-3.0 to 2.3-groovy-3.0 ([#8122](https://github.com/opensearch-project/OpenSearch/pull/8122))
- Bump `com.networknt:json-schema-validator` from 1.0.83 to 1.0.84 (#8141)
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.1.3 to 12.1.4 (#8139)
- Bump `commons-io:commons-io` from 2.12.0 to 2.13.0 in /plugins/discovery-azure-classic ([#8140](https://github.com/opensearch-project/OpenSearch/pull/8140))
- Bump `mockito` from 5.2.0 to 5.4.0 ([#8181](https://github.com/opensearch-project/OpenSearch/pull/8181))
- Bump `netty` from 4.1.93.Final to 4.1.94.Final ([#8191](https://github.com/opensearch-project/OpenSearch/pull/8191))
- Bump `org.apache.hadoop:hadoop-minicluster` from 3.3.5 to 3.3.6 (#8257)
- Bump `io.projectreactor.netty:reactor-netty-http` from 1.1.7 to 1.1.8 (#8256)
- [Upgrade] Lucene 9.7.0 release (#8272)
- Bump `org.jboss.resteasy:resteasy-jackson2-provider` from 3.0.26.Final to 6.2.4.Final in /qa/wildfly ([#8209](https://github.com/opensearch-project/OpenSearch/pull/8209))

### Changed
- Replace jboss-annotations-api_1.2_spec with jakarta.annotation-api ([#7836](https://github.com/opensearch-project/OpenSearch/pull/7836))
- Reduce memory copy in zstd compression ([#7681](https://github.com/opensearch-project/OpenSearch/pull/7681))
- Add min, max, average and thread info to resource stats in tasks API ([#7673](https://github.com/opensearch-project/OpenSearch/pull/7673))
- Add ZSTD compression for snapshotting ([#2996](https://github.com/opensearch-project/OpenSearch/pull/2996))
- Change `com.amazonaws.sdk.ec2MetadataServiceEndpointOverride` to `aws.ec2MetadataServiceEndpoint` ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Change `com.amazonaws.sdk.stsEndpointOverride` to `aws.stsEndpointOverride` ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Add new query profile collector fields with concurrent search execution ([#7898](https://github.com/opensearch-project/OpenSearch/pull/7898))
- Align range and default value for deletes_pct_allowed in merge policy ([#7730](https://github.com/opensearch-project/OpenSearch/pull/7730))
- Rename QueryPhase actors like Suggest, Rescore to be processors rather than phase ([#8025](https://github.com/opensearch-project/OpenSearch/pull/8025))
- Compress and cache cluster state during validate join request ([#7321](https://github.com/opensearch-project/OpenSearch/pull/7321))
- [Snapshot Interop] Add Changes in Create Snapshot Flow for remote store interoperability. ([#7118](https://github.com/opensearch-project/OpenSearch/pull/7118))
- Allow insecure string settings to warn-log usage and advise to migration of a newer secure variant ([#5496](https://github.com/opensearch-project/OpenSearch/pull/5496))

### Deprecated

### Removed
- Remove `COMPRESSOR` variable from `CompressorFactory` and use `DEFLATE_COMPRESSOR` instead ([7907](https://github.com/opensearch-project/OpenSearch/pull/7907))

### Fixed
- Fixing error: adding a new/forgotten parameter to the configuration for checking the config on startup in plugins/repository-s3 #7924
- Enforce 512 byte document ID limit in bulk updates ([#8039](https://github.com/opensearch-project/OpenSearch/pull/8039))
- With only GlobalAggregation in request causes unnecessary wrapping with MultiCollector ([#8125](https://github.com/opensearch-project/OpenSearch/pull/8125))
- Fix mapping char_filter when mapping a hashtag ([#7591](https://github.com/opensearch-project/OpenSearch/pull/7591))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.8...2.x
