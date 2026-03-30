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
    public void receiveAloRecordsUpTo_givenEarliestToLatestReception_expectsSnapshot() {
        String topic = UUID.randomUUID().toString();
        String groupId = AloKafkaBoundedReceiverTest.class.getSimpleName() + UUID.randomUUID();

        List<Long> producedValues = produceRecordsFromValues(
                newProducerConfigs(LongSerializer.class, LongSerializer.class),
                createRandomLongValues(1, Collectors.toList()),
                value -> new ProducerRecord<>(topic, 0, value, value));

        List<Long> result =
                receiveAloValuesAsLongs(topic, groupId, OffsetResetStrategy.EARLIEST, OffsetCriteria.latest());

        assertEquals(producedValues, result);
    }

    @Test
    public void receiveAloRecordsUpTo_givenLatestReceptionAndGroupHasNotBeenUsed_expectsNoReception() {
        String topic = UUID.randomUUID().toString();
        String groupId = AloKafkaBoundedReceiverTest.class.getSimpleName() + UUID.randomUUID();

        produceRecordsFromValues(
                newProducerConfigs(LongSerializer.class, LongSerializer.class),
                createRandomLongValues(4, Collectors.toList()),
                value -> new ProducerRecord<>(topic, 0, value, value));

        List<Long> noneToLatest =
                receiveAloValuesAsLongs(topic, groupId, OffsetResetStrategy.NONE, OffsetCriteria.latest());
        List<Long> latestToLatest =
                receiveAloValuesAsLongs(topic, groupId, OffsetResetStrategy.LATEST, OffsetCriteria.latest());

        assertTrue(noneToLatest.isEmpty());
        assertTrue(latestToLatest.isEmpty());
    }

    @Test
    public void receiveAloRecordsUpTo_givenEarliestToLatestAfterGroupHasBeenUsed_expectsNoRepeatedReception() {
        String topic = UUID.randomUUID().toString();
        String groupId = AloKafkaBoundedReceiverTest.class.getSimpleName() + UUID.randomUUID();

        List<Long> producedValues = produceRecordsFromValues(
                newProducerConfigs(LongSerializer.class, LongSerializer.class),
                createRandomLongValues(4, Collectors.toList()),
                value -> new ProducerRecord<>(topic, 0, value, value));

        List<Long> firstResult =
                receiveAloValuesAsLongs(topic, groupId, OffsetResetStrategy.EARLIEST, OffsetCriteria.latest());
        List<Long> secondResult =
                receiveAloValuesAsLongs(topic, groupId, OffsetResetStrategy.EARLIEST, OffsetCriteria.latest());

        assertEquals(producedValues, firstResult);
        assertTrue(secondResult.isEmpty());
    }

    @Test
    public void receiveAloRecordsUpTo_givenEarliestToLatestAfterNoCommittedPriorUsage_expectsSnapshot() {
        String topic = UUID.randomUUID().toString();
        String groupId = AloKafkaBoundedReceiverTest.class.getSimpleName() + UUID.randomUUID();

        List<Long> producedValues = produceRecordsFromValues(
                newProducerConfigs(LongSerializer.class, LongSerializer.class),
                createRandomLongValues(1, Collectors.toList()),
                value -> new ProducerRecord<>(topic, 1, value, value));

        List<Long> noneToLatest =
                receiveAloValuesAsLongs(topic, groupId, OffsetResetStrategy.NONE, OffsetCriteria.latest());
        List<Long> earliestToLatest =
                receiveAloValuesAsLongs(topic, groupId, OffsetResetStrategy.EARLIEST, OffsetCriteria.latest());

        assertTrue(noneToLatest.isEmpty());
        assertEquals(producedValues, earliestToLatest);
    }

    private List<Long> receiveAloValuesAsLongs(
            String topic, String groupId, OffsetResetStrategy resetStrategy, OffsetCriteria maxInclusive) {
        return newConsumerConfigSource(groupId, ByteArrayDeserializer.class, LongDeserializer.class)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetStrategy.toString())
                .as(AloKafkaBoundedReceiver::<Object, Long>create)
                .receiveAloRecordsUpTo(topic, maxInclusive)
                .consumeAloAndGet(Alo::acknowledge)
                .map(ConsumerRecord::value)
                .collectList()
                .block();
    }

    private static KafkaConfigSource newProducerConfigs(
            Class<? extends Serializer<?>> keySerializer, Class<? extends Serializer<?>> valueSerializer) {
        return newBaseConfigSource()
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
    }

    private static KafkaConfigSource newConsumerConfigSource(
            String groupId,
            Class<? extends Deserializer<?>> keyDeserializer,
            Class<? extends Deserializer<?>> valueDeserializer) {
        return newBaseConfigSource()
                .with(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName())
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
    }

    private static KafkaConfigSource newBaseConfigSource() {
        return KafkaConfigSource.useClientIdAsName()
                .withClientId(KafkaBoundedReceiverTest.class.getSimpleName())
                .withBootstrapServers(BOOTSTRAP_CONNECT);
    }

    private static <T, C extends Collection<T>> C produceRecordsFromValues(
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

    private static <T> T createRandomLongValues(int count, Collector<Long, ?, T> collector) {
        Random random = new Random();
        return IntStream.range(0, count).mapToObj(unused -> random.nextLong()).collect(collector);
    }
}
