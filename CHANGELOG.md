# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

# [0.19.0]
atleon `0.19.0` is a beta release containing updates outlined below

### Additions
* [#202] - Acknowledgement queuing (for log-based receiving) now supports "compaction"
* [#212] - Avro SerDes now support `avro.use.logical.type.converters`
* [#216] - Avro SerDes now support `avro.remove.java.properties`
* [#220] - Add `AloFlux::ofType`
* [#223] - Acknowledgement queue mode made configurable for `AloKafkaReceiver`

### Fixes
* [#208] - Re-completions of queued in-flight acknowledgements are now ignored
* [#214] - Avro SerDes now identify types with generic supertypes as themselves generic
* [#218] - Avro SerDes do not attempt to load reader schema when deserializing as generic data (records/maps)

# [0.18.1]
atleon `0.18.1` is a beta release containing updates outlined below

### Fixes
* [#195] - Guard against `null` data on Kafka deserialization for JSON and Protobuf
* [#208] - Setting of `AcknowledgementQueue.InFlight.error` made to be volatile and atomic
* `reactor-kafka` bumped to 1.3.18

# [0.18.0]
atleon `0.18.0` is a beta release containing updates outlined below

### Additions
* [#204] - Implemented fluent `Alo` error delegation; Supports use cases such as deadlettering

### Fixes
* Standardized `Config` creation convention(s)

# [0.17.1]
atleon `0.17.1` is a beta release containing updates outlined below

### Fixes
* [#200] - Loading (including auto-loading) of instances can be overridden to empty lists by providing `null` or empty string

# [0.17.0]
atleon `0.17.0` is a beta release containing updates outlined below

### Additions
* [#191] - Implemented export of metrics concerning `Alo` queueing
* [#192] - Refactored Avro and Confluent Schema Registry integration to make serialization with either/both available for all infrastructures
* [#195] - Added new modules and support for Protobuf and JSON (Jackson) serialization

### Removals
* [#189] - Removed dependency on `io.confluent:kafka-avro-serializer`

# [0.16.1]
atleon `0.16.1` is a beta release containing updates outlined below

### Fixes
* [#186] - Removed usage of `Void` type in Kotlin extensions; Replace `suspendConsume` with `suspendMap`

# [0.16.0]
atleon `0.16.0` is a beta release containing updates outlined below

### Additions
* [#183] - Added `atleon-kotlin` module with extension functions for coroutines and flows

# [0.15.3]
atleon `0.15.3` is a beta release containing updates outlined below

### Fixes
* [#180] - `AloFailureStrategy` is now applied on publishing of `Alo` (i.e. through `concatMap`, `flatMap`, etc.)

# [0.15.2]
atleon `0.15.2` is a beta release containing updates outlined below

### Fixes
* [#104] - Only query presence of span contexts once in Opentracing decoration
* [#177] - Remove `AloFlux` `groupBy` methods where cardinality is not explicitly provided

# [0.15.1]
atleon `0.15.1` is a beta release containing updates outlined below

### Additions
* [#169] - Implement consistent and configurable `AloFailureStrategy` for dealing with errors resulting from `Alo` processing
* [#176] - Indicate when `ReceivedRabbitMQMessage` is a redelivery

### Fixes
* [#167] - Make best effort to successfully emit errors when configured to do so in Receivers
* [#171] - Avoid unbounded resource usage in `SqsReceiver` by not marking "not-in-flight" until reception of terminal response
* [#172] - Encapsulate config for Kafka resources as `KafkaConfig`

# [0.15.0]
atleon `0.15.0` is a beta release containing updates outlined below

### Additions
* [#152] - Allow subscribing to Kafka topics that match a `Pattern`
* [#154] - Provide terminal `consume` operator on `AloFlux`
* [#156] - Nacknowledger behavior now configurable for Kafka

### Fixes
* [#153] - Avoid nacknowledging if error will be emitted anyway
* [#158] - Close RabbitMQ Receiver after every `AloFlux` termination
* [#162] - Always execute queued acknowledgements in order, regardless of positive vs. negative
* [#164] - Remove custom error handling in `AloRabbitMQSender`

# [0.14.2]
atleon `0.14.2` is a beta release containing updates outlined below

### Fixes
* [#149] - Extract raw type from parameterized stream config type

# [0.14.1]
atleon `0.14.1` is a beta release containing updates outlined below

### Fixes
* [#149] - Stream auto configuration now works with parameterized stream configs

# [0.14.0]
atleon `0.14.0` is a beta release containing updates outlined below

### Additions
* [#136], [#146] - Added `AloSignalListenerFactory` and Micrometer instrumentation for Alo pipeline throughput metering

### Fixes
* [#134] - Parameters used only by decorators have moved from consumed messages to configuration
* [#138] - Standardized observation (metrics, tracing) naming
* [#142] - No longer load `noOp` decorators when auto decorators explicitly configured, but none available
* [#144] - Upgrade Reactor to v3.5.3

# [0.13.0]
atleon `0.13.0` is a beta release containing updates outlined below

### Additions
* [#129] - Implemented annotation-based binding of `AloStream` and `AloStreamConfig` to Spring applications

### Fixes
* [#131] - Deprecated `BLACKLIST` and `WHITELIST` in favor of `BLOCKLIST` and `ALLOWLIST`
* Moved more documentation to [GitHub Wiki](../../wiki)

# [0.12.1]
atleon `0.12.1` is a beta release containing updates outlined below

### Fixes
* [#124] - Added `atleon-micrometer-auto` to bom

# [0.12.0]
atleon `0.12.0` is a beta release containing updates outlined below

### Additions
* [#122] - `ReceivedSqsMessage` now indicates the URL of the queue from which it came
* [#124] - `AloDecorator` implemented to add metrics for `Alo` throughput and processing duration

### Fixes
* [#121] - `AloRabbitMQReceiver` now produces `ReceivedRabbitMQMessage` which indicates the queue it came from

# [0.11.0]
atleon `0.11.0` is a beta release containing updates outlined below

### Additions
* [#114] - `AloRabbitMQReceiver` now supports `AcknowledgerFactory` pattern
* [#110] - Nullability and Non-Nullability annotation support added; Makes Kotlin experience better

### Fixes
* [#107] - `AloFactory` loading now centralized to `AloFactoryConfig`
* [#109] - ConfigLoading now avoids unnecessary parsing when config values are already the desired type
* [#112] - Environmentally loaded configs correctly sanitize environment variables
* [#115] - Deduplication concurrency defaults to infinity
* [#117] - Removed unnecessary use of custom Schedulers

# [0.10.1]
atleon `0.10.1` is a beta release containing updates outlined below

### Fixes
* [#104] - Link to active span context when available and its trace ID is equal to the extracted trace ID

# [0.10.0]
atleon `0.10.0` is a beta release containing updates outlined below

### Additions
* [#86] - Added integration with Opentracing

### Fixes
* [#97] - Default to "auto" loading through ServiceLoader when decorators are not explicitly configured
* [#89] - Make SNS+SQS message publishing instrumentation compatible with asynchronous message emission

# [0.9.1]
atleon `0.9.1` is a beta release containing updates outlined below

### Additions
* [#91] - Added ability for `Alo` implementations to distinguish fan-in propagation from regular propagation
* [#97] - Introduced `Alo` decoration pattern through `AloDecorator`

### Fixes
* [#94] - Added `DelegatingAlo` interface and used it to remove unnecessary `Alo::map` from `AloFlux.mapNotNull` and `AloFlux.mapPresent`

### Removals
* [#89] - Removed unused/unnecessary `SendInterceptor` patterns in Senders
* [#92] - Removed redundant implementations of `Alo`
* [#99] - Removed compile dependency on SLF4J from Micrometer module

# [0.9.0]
atleon `0.9.0` is a beta release containing updates outlined below

### Additions
* [#78] - Added new `Contextual` interface extended by `Alo`
* [#78] - Add `KafkaSendInterceptor`

### Fixes
* [#76] - Replace and deprecate `AloExtendedFlux` with `GroupFlux`
* [#81] - Isolate rate limiting on blocking-friendly Scheulder

# [0.8.3]
atleon `0.8.3` is a beta release containing updates outlined below

### Additions
* [#73] - All results produced from `Alo` Senders implement `SenderResult`

# [0.8.2]
atleon `0.8.2` is a beta release containing updates outlined below

### Fixes
* [#70] - `SqsReceiver::Poller` now simply implements `Subscriber`

# [0.8.1]
atleon `0.8.1` is a beta release containing updates outlined below

### Fixes
* [#67] - `AloKafkaReceiver` and `AloKafkaSender` no longer publish on custom Schedulers

# [0.8.0]
atleon `0.8.0` is a beta release containing updates outlined below

### Removals
* [#64] - Removed `AloMono`

### Fixes
* Rename `DrainableQueue` to `SerialQueue`
* Log warning when Alo items emitted in AloFlux are acknowledged by default

# [0.7.2]
atleon `0.7.2` is a beta release containing updates outlined below

### Additions
* [#59] - All Alo Senders now have single-message sending methods

### Fixes
* [#57] - SQS Message deletion is now non-blocking and synchronization-free
* [#61] - Added missing Javadoc for (most) Alo Senders and Receivers

# [0.7.1]
atleon `0.7.1` is a beta release containing updates outlined below

### Changes
* [#43] - SqsReceiver uses long polling by default

### Fixes
* [#43] - SqsReceiver.Poller sets onCancel before onRequest

# [0.7.0]
atleon '0.7.0' is a beta release containing updates outlined below

### Additions
* [#43] - Implemented integration with SQS. Messages can be sent to and received from SQS queues with at-least-once guarantee
* [#44] - Implemented integration with SNS. Messages can be sent to SNS topics with at-least-once guarantee
* [#48] - All AloSender implementations are now `Closeable`
* [#50] - Added Spring examples for SNS sending and SQS receiving

### Changes
* [#45] - `Configurable` interface moved from `rabbitmq` to `util` for reuse among all modules

# [0.6.0]
atleon '0.6.0' is a beta release containing updates outlined below

### Additions
* [#35] - Added RabbitMQ Route Initialization utilities
* [#36] - Added examples for intended usage of RabbitMQ in Spring applications

### Changes
* [#34] - Fixed type erasure of FloFlux subscribe methods that take Consumers
* [#37] - AloExtendedFlux no longer extends FluxOperator

# [0.5.0]

atleon '0.5.0' is a beta release containing updates outlined below

### Additions

* [#31] - Implement `AloFlux::mapNotNull` and `AloFlux::mapPresent`

# [0.4.0]

atleon '0.4.0' is a beta release containing updated outlined below

### Additions

* [#23] - Enable management of Streams when used with Spring
* [NO_ISSUE] - Add single-topic convenience methods to AloKafkaReceiver
* [#28] - Add `AloFlux::groupByNumberHash`

### Changes

* [#26] - AloStreamConfigs now have default naming

# [0.3.0]

atleon '0.3.0' is a beta release containing updates outlined below

### Additions

* Polling module that can be used to reactively define sources of data backed by periodic polling

# [0.2.0]

atleon `0.2.0` is a beta release containing updates outlined below

### Changes

* Kafka version bumped to 2.8.1
* Confluent version bumped to 6.2.2

# [0.0.1]

atleon `0.0.1` is a beta release. It is the first release of the library.
