package io.atleon.examples.spring.kafka;

import io.atleon.core.AloStream;
import io.atleon.kafka.AloKafkaSender;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.Function;

public class ExampleKafkaGeneration extends AloStream<ExampleKafkaGenerationConfig> {

    @Override
    protected Disposable startDisposable(ExampleKafkaGenerationConfig config) {
        KafkaConfigSource senderConfig = config.getBaseKafkaConfig()
            .withClientId(ExampleKafkaGeneration.class.getSimpleName())
            .withProducerOrderingAndResiliencyConfigs()
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(LongSerializer.class)
            .with(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());

        AloKafkaSender<Long, Long> sender = AloKafkaSender.from(senderConfig);

        return Flux.interval(Duration.ofMillis(100))
            .transform(sender.sendValues(config.getTopic(), Function.identity()))
            .subscribe();
    }
}
