package io.atleon.examples.spring.kafka;

import io.atleon.core.Alo;
import io.atleon.core.AloStream;
import io.atleon.kafka.AloKafkaReceiver;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.micrometer.AloKafkaMetricsReporter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import reactor.core.Disposable;

import java.util.Collections;

public class ExampleKafkaProcessing extends AloStream<ExampleKafkaProcessingConfig> {

    @Override
    protected Disposable startDisposable(ExampleKafkaProcessingConfig config) {
        KafkaConfigSource receiverConfig = config.getBaseKafkaConfig()
            .withClientId(ExampleKafkaProcessing.class.getSimpleName())
            .withConsumerGroupId(ExampleKafkaProcessing.class.getSimpleName())
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .with(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AloKafkaMetricsReporter.class.getName());

        return AloKafkaReceiver.<Long>forValues(receiverConfig)
            .receiveAloValues(Collections.singletonList(config.getTopic()))
            .filter(this::isPrime)
            .map(primeNumber -> "Found a prime number: " + primeNumber)
            .doOnNext(config.getConsumer())
            .subscribe(Alo::acknowledge);
    }

    private boolean isPrime(Number number) {
        long safeValue = Math.abs(number.longValue());
        for (long i = 2; i <= Math.sqrt(safeValue); i++) {
            if (safeValue % i == 0) {
                return false;
            }
        }
        return safeValue > 0;
    }
}
