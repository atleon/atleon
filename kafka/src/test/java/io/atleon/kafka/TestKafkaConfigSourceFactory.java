package io.atleon.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class TestKafkaConfigSourceFactory {

    private TestKafkaConfigSourceFactory() {

    }

    public static KafkaConfigSource createSource(String bootstrapServers) {
        return new KafkaConfigSource()
            .with(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            .with(CommonClientConfigs.CLIENT_ID_CONFIG, "TEST_CLIENT")
            .with(ConsumerConfig.GROUP_ID_CONFIG, "TEST_GROUP")
            .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase())
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }
}
