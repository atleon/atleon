# Atleon Examples
This module contains Atleon streaming examples. Each example class is a runnable streaming example.

## End to End to End
The following Kafka [End to End to End](core/src/main/java/io/atleon/examples/endtoendtoend) sequence shows the incremental steps necessary to reactively produce messages to a Kafka Cluster and apply downstream streaming operations
- [Kafka Part 1](core/src/main/java/io/atleon/examples/endtoendtoend/KafkaPart1.java): Use a Kafka Sender to produce records to an embedded Kafka Broker
- [Kafka Part 2](core/src/main/java/io/atleon/examples/endtoendtoend/KafkaPart2.java): Consume sent records using Kafka Receiver
- [Kafka Part 3](core/src/main/java/io/atleon/examples/endtoendtoend/KafkaPart3.java): Extend Record consumption to stream process with at-least-once processing
- [Kafka Part 4](core/src/main/java/io/atleon/examples/endtoendtoend/KafkaPart4.java): Add another downstream consumer of processed results

## Infrastructural Interoperation
[RabbitmqToKafka](core/src/main/java/io/atleon/examples/infrastructuralinteroperability/RabbitMQToKafka.java) shows how Atleon allows interoperability between RabbitMQ (as a source/Publisher) and Kafka (as a sink/Subscriber) while maintaining at-least-once processing guarantee. For completeness, [KafkaToRabbitMQ](core/src/main/java/io/atleon/examples/infrastructuralinteroperability/KafkaToRabbitMQ.java) shows the inverse, still maintaining at-least-once guarantees.

## Parallelism
[Kafka Topic Partition Parallelism](core/src/main/java/io/atleon/examples/parallelism/KafkaTopicPartitionParallelism.java) shows how to parallelize processing of Kafka Records and subsequent transformations by grouping of Topic-Partitions and assigning a Thread per group.
 
[Kafka Arbitrary Parallelism](core/src/main/java/io/atleon/examples/parallelism/KafkaArbitraryParallelism.java) shows how to parallelize processing of Kafka Records and subsequent transformations by applying arbitrary grouping and assigning a Thread per group. This example (as well as the previous one) highly leverage built-in [Acknowledgement Queueing](../core/src/main/java/io/atleon/core/AloQueueingSubscriber.java) to guarantee in-order acknowledgement of Record offsets.

## Error Handling
[Kafka Error Handling](core/src/main/java/io/atleon/examples/errorhandling/KafkaErrorHandling.java) shows how to apply resiliency to the processing of Kafka Records, both to possible upstream errors and downstream Acknowledgement Errors.

## Deduplication
[Kafka Deduplication](core/src/main/java/io/atleon/examples/deduplication/KafkaDeduplication.java) shows how to add deduplication to the processing of a Kafka topic. This example maintains the incorporation of [Acknowledgement](../core/src/main/java/io/atleon/core/Alo.java) propagation such as to maintain at-least-once processing guarantee

## Metrics
[Kafka Micrometer](core/src/main/java/io/atleon/examples/metrics/KafkaMicrometer.java) shows how Atleon integrates with Micrometer to provide Metrics from Atleon streams, as well as bridging native Kafka metrics to Micrometer

## Tracing
[Kafka Opentracing](core/src/main/java/io/atleon/examples/tracing/KafkaOpentracing.java) shows how Atleon integrates with Opentracing to provide traces in reactive pipelines

## Spring
[Example Kafka Application](spring/src/main/java/io/atleon/examples/spring/kafka/ExampleKafkaApplication.java) demonstrates general intended usage of Atleon in Spring (Boot) applications
[Example RabbitMQ Application](spring/src/main/java/io/atleon/examples/spring/rabbitmq/ExampleRabbitMQApplication.java) demonstrates general intended usage of Atleon in Spring (Boot) applications