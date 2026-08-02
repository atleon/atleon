package io.atleon.micrometer.observation;

import io.atleon.kafka.KafkaReceiverRecord;
import io.atleon.util.ConfigLoading;
import io.micrometer.common.KeyValue;
import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.ReceiverContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * Context associated with the consumption of data from Kafka. Note that this implementation uses
 * only {@link Headers} as the carrier object, rather than (e.g.) an entire consumer record; This
 * is because it is very possible that the lifecycle of observation/processing extends beyond the
 * visibility lifecycle of what's actually consumed.
 */
public abstract class KafkaConsumeContext extends ReceiverContext<Headers> {

    private final @Nullable String clientId;

    private final String topic;

    private final int partition;

    private final long offset;

    private KafkaConsumeContext(Factory factory, ConsumerRecord<?, ?> consumerRecord) {
        super(KafkaConsumeContext::extractLastHeaderValue, Kind.CONSUMER);
        this.clientId = factory.clientId;
        this.topic = consumerRecord.topic();
        this.partition = consumerRecord.partition();
        this.offset = consumerRecord.offset();
        setCarrier(consumerRecord.headers());
    }

    public static KafkaConsumeContext process(KafkaReceiverRecord<?, ?> receiverRecord) {
        return newFactory().process(receiverRecord);
    }

    public static Factory newFactory() {
        return new Factory(null);
    }

    public abstract KeyValue operationNameValue(KeyName name);

    public abstract KeyValue operationTypeValue(KeyName name);

    public Optional<KeyValue> clientIdValue(KeyName name) {
        return clientId != null ? Optional.of(name.withValue(clientId)) : Optional.empty();
    }

    public KeyValue topicValue(KeyName name) {
        return name.withValue(topic);
    }

    public KeyValue partitionValue(KeyName name) {
        return name.withValue(Long.toString(partition));
    }

    public KeyValue offsetValue(KeyName name) {
        return name.withValue(Long.toString(offset));
    }

    private static @Nullable String extractLastHeaderValue(Headers carrier, String key) {
        Header header = carrier.lastHeader(key);
        return header != null && header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    public static final class Polled extends KafkaConsumeContext {

        private Polled(Factory factory, ConsumerRecord<?, ?> consumerRecord) {
            super(factory, consumerRecord);
        }

        @Override
        public KeyValue operationNameValue(KeyName name) {
            return name.withValue("polled");
        }

        @Override
        public KeyValue operationTypeValue(KeyName name) {
            return name.withValue("receive");
        }
    }

    public static final class Process extends KafkaConsumeContext {

        private Process(Factory factory, ConsumerRecord<?, ?> consumerRecord) {
            super(factory, consumerRecord);
        }

        @Override
        public KeyValue operationNameValue(KeyName name) {
            return name.withValue("process");
        }

        @Override
        public KeyValue operationTypeValue(KeyName name) {
            return name.withValue("process");
        }
    }

    public static final class Factory {

        private final @Nullable String clientId;

        private Factory(@Nullable String clientId) {
            this.clientId = clientId;
        }

        public KafkaConsumeContext polled(ConsumerRecord<?, ?> consumerRecord) {
            return new Polled(this, consumerRecord);
        }

        public KafkaConsumeContext process(KafkaReceiverRecord<?, ?> receiverRecord) {
            return process(receiverRecord.consumerRecord());
        }

        public KafkaConsumeContext process(ConsumerRecord<?, ?> consumerRecord) {
            return new Process(this, consumerRecord);
        }

        public Factory withConsumerProperties(Map<String, ?> consumerProperties) {
            Factory result = this;
            result = ConfigLoading.loadString(consumerProperties, ConsumerConfig.CLIENT_ID_CONFIG)
                    .map(result::withClientId)
                    .orElse(result);
            return result;
        }

        public Factory withClientId(String clientId) {
            return new Factory(clientId);
        }
    }
}
