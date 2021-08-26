package io.atleon.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class TestKafkaConfigFactory {

    private TestKafkaConfigFactory() {

    }

    public static KafkaConfigSource createSource(String bootstrapServers) {
        KafkaConfigSource kafkaConfigFactory = new KafkaConfigSource();
        kafkaConfigFactory.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaConfigFactory.put(CommonClientConfigs.CLIENT_ID_CONFIG, "TEST_CLIENT");
        kafkaConfigFactory.put(ConsumerConfig.GROUP_ID_CONFIG, "TEST_GROUP");
        kafkaConfigFactory.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        kafkaConfigFactory.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConfigFactory.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConfigFactory.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConfigFactory.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return kafkaConfigFactory;
    }
}
