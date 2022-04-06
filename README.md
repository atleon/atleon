# Atleon
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![javadoc](https://javadoc.io/badge2/io.atleon/atleon-core/javadoc.svg)](https://javadoc.io/doc/io.atleon/atleon-core)
[![main build workflow](https://github.com/atleon/atleon/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/atleon/atleon/actions/workflows/build.yml)


Atleon is a [Reactive Streams](https://www.reactive-streams.org/) library aimed at satisfying table stakes requirements of infinite stream processing use cases. One such typical use case is streaming from one message broker (i.e. [Kafka](https://kafka.apache.org/) or [RabbitMQ](https://www.rabbitmq.com/)), enriching the data, and producing to some other message broker.

As much as possible, Atleon is backed by [Project Reactor](https://projectreactor.io/) and its extensions (like [Reactor Kafka](https://github.com/reactor/reactor-kafka) and [Reactor RabbitMQ](https://github.com/reactor/reactor-rabbitmq)). Usage of Project Reactor allows Atleon to maintain the inherent developmental attributes afforded by the Reactive Streams specification:
* Non-blocking backpressure
* Arbitrary parallelism
* Infrastructural Interoperability (Kafka, RabbitMQ, etc.)
* Implementation Interoperability (Project Reactor, [ReactiveX/RxJava](https://github.com/ReactiveX/RxJava), etc.)

On top of the above attributes, the value that Atleon brings to the table is the guarantee of "At Least Once" processing. In the face of thread switching that typically happens in reactive pipelines, guaranteeing that any given received message is fully processed before acknowledging that message back to the source (i.e. committing a Kafka Record's offset, or ack'ing a RabbitMQ Delivery) is a tricky challenge. However, Atleon makes handling this concern as easy and as transparent as possible.

## TL;DR

If Atleon does only _one_ thing, it is to make it possible to implement infinite data processing pipelines using Reactive Streams (and implementations like Project Reactor) without having to explicitly manage message acknowledgement, while guaranteeing that every message is fully processed at least once. 

## At Least Once Processing Guarantee

In typical use cases, it is required that a message is not abstractly marked "fully processed" at the source from which it came until it is known that the message has been fully processed by whatever pipeline we have defined for it. In Atleon, the concept of marking a message as "fully processed" is called "acknowledgement". Similarly, we typically want some functionality in place for when a message is unable to be "fully processed" due to some fatal error. In Atleon, the termination of a message's processing due to a fatal error is called "nacknowledgement" (negative acknowledgement).

For the purposes of concise documentation, unless otherwise explicitly indicated, we will use the term "acknowledgement" to refer to both positive and negative acknowledgement.

Propagating mechanisms for acknowledgement can be cumbersome in reactive pipelines. Explicitly doing so can make such pipelines less readable and takes away from developers' and readers' abilities to focus on what's being done to the actual data flowing through the pipeline. Atleon provides precisely the abstraction to propagate acknowledgement as data items are transformed. This abstraction is called [Alo](core/src/main/java/io/atleon/core/Alo.java). Implementations of `Alo` handle the responsibility of operating on their contained data item and propagating acknowledgement along with transformations of their data item.

Atleon makes most of the handling of Alo transparent to developers through [AloFlux](core/src/main/java/io/atleon/core/AloFlux.java). Much like Project Reactor's [Flux](https://javadoc.io/doc/io.projectreactor/reactor-core/latest/reactor/core/publisher/Flux.html), `AloFlux` allows developers to define logic purely in terms of data items while wrapping those transformations with appropriate operations on `Alo`.

## Getting Started

Before using Atleon, you should be familiar with [Reactive Streams]() and [Project Reactor]().

Atleon dependencies are available in Maven Central under the following coordinates:

```xml
<dependency>
    <groupId>io.atleon</groupId>
    <artifactId>atleon-core</artifactId>
    <version>${atleon.version}</version>
</dependency>
<dependency>
    <groupId>io.atleon</groupId>
    <artifactId>atleon-kafka</artifactId>
    <version>${atleon.version}</version>
</dependency>
...
```

The typical entry points in to Atleon are through Receiver and Sender implementations. The following example shows how to create a reactive Kafka stream:

```java
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.DefaultAloKafkaSenderResultSubscriber;
import io.atleon.kafka.KafkaConfigSource;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.function.Function;

public class GettingStarted {
    
    public static void main(String[] args) {
        //Step 1) Create Kafka Config for Consumer that backs Receiver
        KafkaConfigSource kafkaReceiverConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, GettingStarted.class.getSimpleName())
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            .with(ConsumerConfig.GROUP_ID_CONFIG, GettingStarted.class.getSimpleName())
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 2) Create Kafka Config for Producer that backs Sender
        KafkaConfigSource kafkaSenderConfig = KafkaConfigSource.useClientIdAsName()
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, GettingStarted.class.getSimpleName())
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
            .with(ProducerConfig.ACKS_CONFIG, "all");

        //Step 3) Define Kafka streaming pipeline and subscribe
        AloKafkaReceiver.<String>forValues(kafkaReceiverConfig)
            .receiveAloValues(Collections.singletonList("topic1"))
            .map(String::toUpperCase)
            .map(string -> new ProducerRecord("topic2", string, string))
            .transform(AloKafkaSender.<String, String>from(kafkaSenderConfig)::sendAloRecords)
            .subscribe(new DefaultAloKafkaSenderResultSubscriber<>());
        
        // ... Do more things while the above stream process is running ...
    }
}
```

Note that more examples are available and runnable in the [Examples Module](examples).

#### Building

Atleon is built using Maven. Installing Maven locally is optional as you can use the Maven Wrapper:

```$bash
./mvnw clean verify
```

## Contributing

Please refer to [CONTRIBUTING](CONTRIBUTING.md) for information on how to contribute to Atleon

## Legal

This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).