# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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