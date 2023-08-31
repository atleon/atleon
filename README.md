# Atleon
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Main Build Workflow](https://github.com/atleon/atleon/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/atleon/atleon/actions/workflows/build.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.atleon/atleon-core/badge.svg?style=plastic)](https://mvnrepository.com/artifact/io.atleon)
[![Javadoc](https://javadoc.io/badge2/io.atleon/atleon-core/javadoc.svg)](https://javadoc.io/doc/io.atleon/atleon-core)

Atleon is reactive message processing framework based on [Reactive Streams](https://www.reactive-streams.org/) and backed by [Project Reactor](https://projectreactor.io/).

The primary goal of Atleon is to make it straightforward to implement infinite message processing pipelines compatible with non-blocking APIs, integrable with popular message brokers (like [Kafka](https://kafka.apache.org/), [RabbitMQ](https://www.rabbitmq.com/), etc.), while _seamlessly_ maintaining **at** **le**ast **on**ce processing guarantees.


## Documentation and Getting Started
Atleon documentation and instructions on how to get started are available in the [Wiki](../../wiki).

An example message processing pipeline in Atleon looks like the following:

```java
import io.atleon.core.AloStream;
import io.atleon.core.DefaultAloSenderResultSubscriber;
import io.atleon.kafka.AloKafkaSender;
import reactor.core.Disposable;

public class MyStream extends AloStream<MyStreamConfig> {

    @Override
    public Disposable startDisposable(MyStreamConfig config) {
        AloKafkaSender<String, String> sender = config.buildKafkaMessageSender();

        return config.buildKafkaMessageReceiver()
            .receiveAloRecords(config.getSourceTopic())
            .map(record -> config.getService().transform(record.value()))
            .filter(message -> !message.isEmpty())
            .transform(sender.sendAloValues(config.getDestinationTopic(), message -> message.substring(0, 1)))
            .resubscribeOnError(config.name())
            .doFinally(sender::close)
            .subscribeWith(new DefaultAloSenderResultSubscriber<>());
    }
}
```

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
