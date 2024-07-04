package io.atleon.kafka;

import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaBoundedReceiverTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    @Test
    public void allDataCurrentlyOnATopicCanBeConsumed() {
        String topic = UUID.randomUUID().toString();
        Set<Long> producedValues = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            createRandomLongValues(1000, Collectors.toSet()),
            value -> new ProducerRecord<>(topic, value, value)
        );

        OffsetRange offsetRange = OffsetCriteria.earliest().to(OffsetCriteria.latest());
        Set<Long> result = receiveRecordValues(topic, LongDeserializer.class, offsetRange, Collectors.toSet());

        assertEquals(producedValues, result);
    }

    @Test
    public void theEarliestRecordOnATopicPartitionCanBeConsumed() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            createRandomLongValues(4, Collectors.toList()),
            value -> new ProducerRecord<>(topic, 0, value, value)
        );

        List<Long> result = receiveValuesAsLongs(topic, OffsetCriteria.earliest().asRange());

        assertEquals(producedValues.subList(0, 1), result);
    }

    @Test
    public void theLatestRecordOnATopicPartitionCanBeConsumed() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            createRandomLongValues(4, Collectors.toList()),
            value -> new ProducerRecord<>(topic, 0, value, value)
        );

        List<Long> result = receiveValuesAsLongs(topic, OffsetCriteria.latest().asRange());

        assertEquals(producedValues.subList(producedValues.size() - 1, producedValues.size()), result);
    }

    @Test
    public void recordAtASpecificOffsetCanBeConsumed() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            createRandomLongValues(4, Collectors.toList()),
            value -> new ProducerRecord<>(topic, 0, value, value)
        );

        List<Long> result = receiveValuesAsLongs(topic, OffsetCriteria.raw(1).asRange());

        assertEquals(producedValues.subList(1, 2), result);
    }

    @Test
    public void recordsInASpecificOffsetRangeCanBeConsumed() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            createRandomLongValues(4, Collectors.toList()),
            value -> new ProducerRecord<>(topic, 0, value, value)
        );

        assertEquals(
            producedValues.subList(0, 2),
            receiveValuesAsLongs(topic, OffsetCriteria.raw(0).to(OffsetCriteria.raw(1)))
        );
        assertEquals(
            producedValues.subList(1, 3),
            receiveValuesAsLongs(topic, OffsetCriteria.raw(1).to(OffsetCriteria.raw(2)))
        );
        assertEquals(
            producedValues.subList(2, 4),
            receiveValuesAsLongs(topic, OffsetCriteria.raw(2).to(OffsetCriteria.raw(3)))
        );
        assertEquals(
            producedValues.subList(3, 4),
            receiveValuesAsLongs(topic, OffsetCriteria.raw(3).to(OffsetCriteria.raw(4)))
        );
    }

    @Test
    public void recordAtASpecificTimestampCanBeConsumed() {
        String topic = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        List<Long> producedTimestamps = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            IntStream.range(0, 3).mapToObj(index -> now + (index * 2L)).collect(Collectors.toList()),
            epochMillis -> new ProducerRecord<>(topic, 0, epochMillis, epochMillis, epochMillis)
        );

        List<Long> result = receiveValuesAsLongs(topic, OffsetCriteria.timestamp(producedTimestamps.get(1)).asRange());

        assertEquals(producedTimestamps.subList(1, 2), result);
    }

    @Test
    public void recordsInATimestampRangeCanBeConsumed() {
        String topic = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        List<Long> producedTimestamps = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            IntStream.range(0, 3).mapToObj(index -> now + (index * 2L)).collect(Collectors.toList()),
            epochMillis -> new ProducerRecord<>(topic, 0, epochMillis, epochMillis, epochMillis)
        );

        assertEquals(
            Collections.emptyList(),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(0) - 2, producedTimestamps.get(0) - 1))
        );
        assertEquals(
            producedTimestamps.subList(0, 1),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(0) - 1, producedTimestamps.get(0)))
        );
        assertEquals(
            producedTimestamps.subList(0, 1),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(0) - 1, producedTimestamps.get(0) + 1))
        );
        assertEquals(
            producedTimestamps.subList(0, 1),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(0), producedTimestamps.get(0) + 1))
        );
        assertEquals(
            producedTimestamps.subList(0, 2),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(0), producedTimestamps.get(1)))
        );
        assertEquals(
            producedTimestamps.subList(1, 2),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(1) - 1, producedTimestamps.get(1) + 1))
        );
        assertEquals(
            producedTimestamps.subList(1, 3),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(1), producedTimestamps.get(2)))
        );
        assertEquals(
            producedTimestamps.subList(2, 3),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(2) - 1, producedTimestamps.get(2)))
        );
        assertEquals(
            producedTimestamps.subList(2, 3),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(2) - 1, producedTimestamps.get(2) + 1))
        );
        assertEquals(
            producedTimestamps.subList(2, 3),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(2), producedTimestamps.get(2) + 1))
        );
        assertEquals(
            Collections.emptyList(),
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(2) + 1, producedTimestamps.get(2) + 2))
        );
        assertEquals(
            producedTimestamps,
            receiveValuesAsLongs(topic, timestampRange(producedTimestamps.get(0), producedTimestamps.get(2)))
        );
    }

    @Test
    public void recordsCanBeConsumedFromASingleTopicPartition() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            LongStream.range(0, 6).boxed().collect(Collectors.toList()),
            value -> new ProducerRecord<>(topic, (int) (value / 3), value, value)
        );

        OffsetRangeProvider allValuesFromFirstPartition = OffsetRangeProvider.inOffsetRangeFromTopicPartition(
            new TopicPartition(topic, 0),
            OffsetCriteria.earliest().to(OffsetCriteria.latest())
        );
        OffsetRangeProvider partialValuesFromSecondPartition = OffsetRangeProvider.inOffsetRangeFromTopicPartition(
            new TopicPartition(topic, 1),
            1,
            OffsetCriteria.latest()
        );

        assertEquals(producedValues.subList(0, 3), receiveValuesAsLongs(topic, allValuesFromFirstPartition));
        assertEquals(producedValues.subList(4, 6), receiveValuesAsLongs(topic, partialValuesFromSecondPartition));
    }

    @Test
    public void recordsCanBeConsumedStartingFromASpecificPartitionAndOffset() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            LongStream.range(0, 6).boxed().collect(Collectors.toList()),
            value -> new ProducerRecord<>(topic, (int) (value / 3), value, value)
        );

        OffsetRangeProvider startingInFirstPartition = OffsetRangeProvider.startingFromRawOffsetInTopicPartition(
            new TopicPartition(topic, 0),
            2,
            OffsetCriteria.earliest().to(OffsetCriteria.latest())
        );

        assertEquals(producedValues.subList(2, 6), receiveValuesAsLongs(topic, startingInFirstPartition));
    }

    @Test
    public void recordsCanBeConsumedUsingRawOffsets() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = produceRecordsFromValues(
            newProducerConfigs(LongSerializer.class, LongSerializer.class),
            LongStream.range(0, 6).boxed().collect(Collectors.toList()),
            value -> new ProducerRecord<>(topic, (int) (value / 3), value, value)
        );

        OffsetRangeProvider recordsInFirstPartition = OffsetRangeProvider.usingRawOffsetRange(
            new TopicPartition(topic, 0),
            RawOffsetRange.of(1, 2)
        );

        assertEquals(producedValues.subList(1, 3), receiveValuesAsLongs(topic, recordsInFirstPartition));
    }

    private List<Long> receiveValuesAsLongs(String topic, OffsetRange offsetRange) {
        return receiveValuesAsLongs(topic, OffsetRangeProvider.inOffsetRangeFromAllTopicPartitions(offsetRange));
    }

    private List<Long> receiveValuesAsLongs(String topic, OffsetRangeProvider rangeProvider) {
        return receiveRecordValues(topic, LongDeserializer.class, rangeProvider, Collectors.toList());
    }

    private <T, C extends Collection<T>> C receiveRecordValues(
        String topic,
        Class<? extends Deserializer<T>> deserializer,
        OffsetRange offsetRange,
        Collector<T, ?, C> collector
    ) {
        OffsetRangeProvider rangeProvider = OffsetRangeProvider.inOffsetRangeFromAllTopicPartitions(offsetRange);
        return receiveRecordValues(topic, deserializer, rangeProvider, collector);
    }

    private <T, C extends Collection<T>> C receiveRecordValues(
        String topic,
        Class<? extends Deserializer<T>> deserializer,
        OffsetRangeProvider rangeProvider,
        Collector<T, ?, C> collector
    ) {
        return KafkaBoundedReceiver.<Object, T>create(newConsumerConfigSource(ByteArrayDeserializer.class, deserializer))
            .receiveRecords(Collections.singletonList(topic), rangeProvider)
            .map(ConsumerRecord::value)
            .collect(collector)
            .block(Duration.ofSeconds(30));
    }

    private KafkaConfigSource newProducerConfigs(
        Class<? extends Serializer<?>> keySerializer,
        Class<? extends Serializer<?>> valueSerializer
    ) {
        return newBaseConfigSource()
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName())
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
    }

    private KafkaConfigSource newConsumerConfigSource(
        Class<? extends Deserializer<?>> keyDeserializer,
        Class<? extends Deserializer<?>> valueDeserializer
    ) {
        return newBaseConfigSource()
            .with(ConsumerConfig.GROUP_ID_CONFIG, KafkaBoundedReceiverTest.class.getSimpleName())
            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName())
            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
    }

    private KafkaConfigSource newBaseConfigSource() {
        return KafkaConfigSource.useClientIdAsName()
            .withClientId(KafkaBoundedReceiverTest.class.getSimpleName())
            .withBootstrapServers(BOOTSTRAP_CONNECT);
    }

    private <T, C extends Collection<T>> C produceRecordsFromValues(
        KafkaConfigSource configSource,
        C data,
        Function<T, ProducerRecord<Object, T>> recordCreator
    ) {
        try (AloKafkaSender<Object, T> sender = AloKafkaSender.from(configSource)) {
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

    private static OffsetRange timestampRange(long minEpochMillis, long maxEpochMillis) {
        return OffsetCriteria.timestamp(minEpochMillis).to(OffsetCriteria.timestamp(maxEpochMillis));
    }
}