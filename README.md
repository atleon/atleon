# Atleon
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Atleon is a [Reactive Streams](https://www.reactive-streams.org/) library aimed at satisfying table stakes requirements of infinite stream processing use cases. One such typical use case is streaming from one message broker (i.e. Kafka or RabbitMQ), enriching the data, and producing to some other message broker.

As much as possible, Atleon is backed by [Project Reactor](https://projectreactor.io/). Usage of Project Reactor allows Atleon to maintain the inherent developmental attributes afforded by the Reactive Streams specification:
* Non-blocking backpressure
* Arbitrary parallelism
* Infrastructural Interoperability ([Kafka](https://kafka.apache.org/), [RabbitMQ](https://www.rabbitmq.com/), etc.)
* Implementation Interoperability ([ReactiveX/RxJava](https://github.com/ReactiveX/RxJava), [Project Reactor](https://projectreactor.io/), etc.)

On top of the above attributes, the value that Atleon brings to the table is the guarantee of "At Least Once" processing. In the face of thread switching that typically happens in reactive pipelines, guaranteeing that any given received message is fully processed before acknowledging that message back to the source (i.e. committing a Kafka Record's offset, or ack'ing a RabbitMQ Delivery) is a tricky challenge. However, Atleon makes handling this concern as easy and as transparent as possible.