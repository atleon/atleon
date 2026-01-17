package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloKafkaBoundedReceiverTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    @Test
    public void recordsCanBeConsumedAndCommittedForAConsumerGroup() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues1 = produceRecordsFromValues(
                newProducerConfigs(LongSerializer.class, LongSerializer.class),
                createRandomLongValues(4, Collectors.toList()),
                value -> new ProducerRecord<>(topic, 0, value, value));

        assertTrue(receiveAloValuesAsLongs(topic, OffsetResetStrategy.NONE, OffsetCriteria.latest())
                .isEmpty());
        assertTrue(receiveAloValuesAsLongs(topic, OffsetResetStrategy.LATEST, OffsetCriteria.latest())
                .isEmpty());
        assertEquals(
                producedValues1, receiveAloValuesAsLongs(topic, OffsetResetStrategy.EARLIEST, OffsetCriteria.latest()));
        assertTrue(receiveAloValuesAsLongs(topic, OffsetResetStrategy.EARLIEST, OffsetCriteria.latest())
                .isEmpty());

        List<Long> producedValues2 = produceRecordsFromValues(
                newProducerConfigs(LongSerializer.class, LongSerializer.class),
                createRandomLongValues(1, Collectors.toList()),
                value -> new ProducerRecord<>(topic, 0, value, value));

        assertEquals(
                producedValues2, receiveAloValuesAsLongs(topic, OffsetResetStrategy.EARLIEST, OffsetCriteria.latest()));

        List<Long> producedValues3 = produceRecordsFromValues(
                newProducerConfigs(LongSerializer.class, LongSerializer.class),
                createRandomLongValues(1, Collectors.toList()),
                value -> new ProducerRecord<>(topic, 1, value, value));

        assertTrue(receiveAloValuesAsLongs(topic, OffsetResetStrategy.NONE, OffsetCriteria.latest())
                .isEmpty());
        assertEquals(
                producedValues3, receiveAloValuesAsLongs(topic, OffsetResetStrategy.EARLIEST, OffsetCriteria.latest()));
    }

    private List<Long> receiveAloValuesAsLongs(
            String topic, OffsetResetStrategy resetStrategy, OffsetCriteria maxInclusive) {
        return newConsumerConfigSource(ByteArrayDeserializer.class, LongDeserializer.class)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetStrategy.toString())
                .as(AloKafkaBoundedReceiver::<Object, Long>create)
                .receiveAloRecordsUpTo(topic, maxInclusive)
                .consumeAloAndGet(Alo::acknowledge)
                .map(ConsumerRecord::value)
                .collectList()
                .block();
    }

    private KafkaConfigSource newProducerConfigs(
            Class<? extends Serializer<?>> keySerializer, Class<? extends Serializer<?>> valueSerializer) {
        return newBaseConfigSource()
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
    }

    private KafkaConfigSource newConsumerConfigSource(
            Class<? extends Deserializer<?>> keyDeserializer, Class<? extends Deserializer<?>> valueDeserializer) {
        return newBaseConfigSource()
                .with(ConsumerConfig.GROUP_ID_CONFIG, AloKafkaBoundedReceiverTest.class.getSimpleName())
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName())
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
    }

    private KafkaConfigSource newBaseConfigSource() {
        return KafkaConfigSource.useClientIdAsName()
                .withClientId(KafkaBoundedReceiverTest.class.getSimpleName())
                .withBootstrapServers(BOOTSTRAP_CONNECT);
    }

    private <T, C extends Collection<T>> C produceRecordsFromValues(
            KafkaConfigSource configSource, C data, Function<T, ProducerRecord<Object, T>> recordCreator) {
        try (AloKafkaSender<Object, T> sender = AloKafkaSender.create(configSource)) {
            Flux.fromIterable(data)
                    .map(recordCreator)
                    .transform(sender::sendRecords)
                    .then()
                    .block(Duration.ofSeconds(30));
        }
        return data;
    }

    private <T> T createRandomLongValues(int count, Collector<Long, ?, T> collector) {
        Random random = new Random();
        return IntStream.range(0, count).mapToObj(unused -> random.nextLong()).collect(collector);
    }
}
