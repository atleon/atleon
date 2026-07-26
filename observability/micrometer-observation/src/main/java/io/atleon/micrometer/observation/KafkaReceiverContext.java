package io.atleon.micrometer.observation;

import io.atleon.kafka.KafkaReceiverRecord;
import io.atleon.util.ConfigLoading;
import io.micrometer.observation.transport.ReceiverContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Context associated with the reception of data from Kafka. Note that this implementation uses
 * only the headers (of received records) as the carrier object, rather than (e.g.) an entire
 * consumer record; This is because it is very possible that the lifecycle of
 * observation/processing extends beyond the visibility lifecycle of received records.
 */
public final class KafkaReceiverContext extends ReceiverContext<Headers> {

    private final @Nullable String clientId;

    private final String topic;

    private final int partition;

    private final long offset;

    private KafkaReceiverContext(@Nullable String clientId, ConsumerRecord<?, ?> consumerRecord) {
        super(KafkaReceiverContext::extractLastHeaderValue);
        this.clientId = clientId;
        this.topic = consumerRecord.topic();
        this.partition = consumerRecord.partition();
        this.offset = consumerRecord.offset();
        setCarrier(consumerRecord.headers());
    }

    public static KafkaReceiverContext create(KafkaReceiverRecord<?, ?> receiverRecord) {
        return newFactory().create(receiverRecord);
    }

    public static Factory newFactory() {
        return new Factory(null);
    }

    public @Nullable String getClientId() {
        return clientId;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartitionAsString() {
        return Integer.toString(partition);
    }

    public String getOffsetAsString() {
        return Long.toString(offset);
    }

    private static @Nullable String extractLastHeaderValue(Headers carrier, String key) {
        Header header = carrier.lastHeader(key);
        return header != null && header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    public static final class Factory {

        private final @Nullable String clientId;

        private Factory(@Nullable String clientId) {
            this.clientId = clientId;
        }

        public KafkaReceiverContext create(KafkaReceiverRecord<?, ?> receiverRecord) {
            return create(receiverRecord.consumerRecord());
        }

        public KafkaReceiverContext create(ConsumerRecord<?, ?> consumerRecord) {
            return new KafkaReceiverContext(clientId, consumerRecord);
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
