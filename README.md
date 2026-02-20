# Atleon

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Main Build Workflow](https://github.com/atleon/atleon/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/atleon/atleon/actions/workflows/main.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.atleon/atleon-core)](https://search.maven.org/artifact/io.atleon/atleon-core)
[![Javadoc](https://javadoc.io/badge2/io.atleon/atleon-core/javadoc.svg)](https://javadoc.io/doc/io.atleon/atleon-core)

Atleon is a lightweight reactive stream processing framework that scalably transforms data from any supported infrastructure, and allows sending that data nearly anywhere, while _seamlessly_ maintaining **at** **le**ast **on**ce processing guarantees.

Atleon is based on [Reactive Streams](https://www.reactive-streams.org/) and backed by [Project Reactor](https://projectreactor.io/). There are two levels of client APIs offered:

- **Low-Level**: Low-level client APIs are thin reactive wrappers around third-party native infrastructure clients. These APIs are defined in terms of Reactor-native types (e.g. `Flux` and `Mono`), and emit elements that contain (or otherwise reference) native infrastructure types. Consumption of messages is implemented via `Receiver` implementations, with emitted elements containing callbacks for acknowledgement (both positive/successful and negative/failed). Production of messages is implemented via `Sender` implementations, with emitted elements containing metadata about successes and/or errors indicating production failure.
- **High-Level**: High-Level client APIs decorate low-level clients with an Atleon-native abstraction called `Alo` (short for At Least Once). `Alo` provides per-element context, which always includes acknowledgement functionality, and allows for decorating that context with functionality like metrics, tracing, and application-specific metadata. In order to make working with `Alo` about as simple as working with `Flux`, high-level clients produce a special `AloFlux<T>` type that wraps an underlying `Flux<Alo<T>>`, and allows for defining reactive pipelines purely in terms of data typing (`T`), while providing automatic context propagation.

## Documentation and Getting Started

Atleon documentation and instructions on how to get started are available in the [Wiki](../../wiki).

### Low-level Client Example

The following is an example of using a low-level client in Atleon:

```java
import io.atleon.kafka.KafkaReceiver;
import io.atleon.kafka.KafkaReceiverOptions;
import io.atleon.kafka.KafkaReceiverRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import reactor.core.Disposable;

public class Example {

    public static void main(String[] args) throws Exception {
        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "example")
            .consumerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-id")
            .consumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .consumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .build();

        KafkaReceiver<String, String> receiver = KafkaReceiver.create(receiverOptions);

        Disposable disposable = receiver.receiveRecords("source-topic")
            .doOnNext(it -> System.out.printf("Consumed record with key=%s and value=%s", it.key(), it.value()))
            .subscribe(KafkaReceiverRecord::acknowledge);

        System.in.read(); // Added for posterity - Everything above will execute asynchronously
        disposable.dispose(); // Cleanup
    }
}
```

### High-Level AloStream Example

The next example builds on the availability of low-level clients by switching to high-level `Alo` clients, adding the sending/production of transformed messages, and wrapping the stream definition in an implementation of `AloStream`:

```java
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.Disposable;

public class MyStream extends SelfConfigurableAloStream {

    private final KafkaConfigSource configSource;

    private final String sourceTopic;

    private final String destinationTopic;

    public MyStream(KafkaConfigSource configSource, String sourceTopic, String destinationTopic) {
        this.configSource = configSource;
        this.sourceTopic = sourceTopic;
        this.destinationTopic = destinationTopic;
    }

    @Override
    public Disposable startDisposable() {
        AloKafkaSender<String, String> sender = buildKafkaSender();

        return buildKafkaReceiver()
            .receiveAloRecords(sourceTopic)
            .mapNotNull(it -> it.value() != null ? it.value().toUpperCase() : null) // Business logic goes here
            .transform(sender.sendAloValues(destinationTopic, message -> message.substring(0, 1)))
            .resubscribeOnError(name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }

    private AloKafkaSender<String, String> buildKafkaSender() {
        return configSource
            .withClientId(name())
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class)
            .as(AloKafkaSender::create);
    }

    private AloKafkaReceiver<String, String> buildKafkaReceiver() {
        return configSource
            .withClientId(name())
            .withConsumerGroupId("consumer-group-id")
            .withKeyDeserializer(StringSerializer.class)
            .withValueDeserializer(StringSerializer.class)
            .as(AloKafkaReceiver::create);
    }
}

```

### Spring AloStream Example

Atleon has built-in integration with Spring, where a fully configured AloStream looks like the following:

`pom.xml`:

```xml

<dependencies>
    <dependency>
        <groupId>io.atleon</groupId>
        <artifactId>atleon-kafka</artifactId> <!-- Include infrastructure client -->
        <version>${atleon.version}</version>
    </dependency>
    <dependency>
        <groupId>io.atleon</groupId>
        <artifactId>atleon-spring</artifactId>
        <version>${atleon.version}</version>
    </dependency>
</dependencies>
```

`application.yml`:

```yaml
atleon:
  config.sources:
    - name: kafkaConfigSource
      type: kafka
      bootstrap.servers: localhost:9092

stream:
  kafka:
    destination.topic: output
    source.topic: input
```

`MyStream.java`:

```java
import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStream;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;

@AutoConfigureStream
public class MyStream extends SpringAloStream {

    private final KafkaConfigSource configSource;

    private final MyService service;

    public MyStream(ApplicationContext context) {
        super(context);
        this.configSource = context.getBean("kafkaConfigSource", KafkaConfigSource.class);
        this.service = context.getBean(MyService.class);
    }

    @Override
    public Disposable startDisposable(MyStreamConfig config) {
        AloKafkaSender<String, String> sender = buildKafkaSender();
        String destinationTopic = getRequiredProperty("stream.kafka.destination.topic");

        return buildKafkaReceiver()
            .receiveAloRecords(getRequiredProperty("stream.kafka.source.topic"))
            .mapNotNull(it -> it.value() != null ? it.value().toUpperCase() : null) // Business logic goes here
            .transform(sender.sendAloValues(destinationTopic, message -> message.substring(0, 1)))
            .resubscribeOnError(name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }

    private AloKafkaSender<String, String> buildKafkaSender() {
        return configSource
            .withClientId(name())
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class)
            .as(AloKafkaSender::create);
    }

    private AloKafkaReceiver<String, String> buildKafkaReceiver() {
        return configSource
            .withClientId(name())
            .withConsumerGroupId("consumer-group-id")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .as(AloKafkaReceiver::create);
    }
}
```

### Runnable Examples

The [examples module](examples) contains runnable classes showing Atleon in action and intended usage.

## Building

Atleon is built using Maven. Installing Maven locally is optional as you can use the Maven Wrapper:

```$bash
./mvnw clean verify
```

#### Docker

Atleon makes use of [Testcontainers](https://www.testcontainers.org/) for some unit tests. Testcontainers is based on Docker, so successfully building Atleon requires Docker to be running locally.

## Contributing

Please refer to [CONTRIBUTING](CONTRIBUTING.md) for information on how to contribute to Atleon

## Legal

This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
