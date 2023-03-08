# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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