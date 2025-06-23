package io.atleon.kafka;

import io.atleon.util.Proxying;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * A facade around an active {@link Producer} being used for sending.
 *
 * @param <K> The type of keys in records sent by this producer
 * @param <V> The type of values in records sent by this producer
 */
final class SendingProducer<K, V> implements ProducerInvocable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);

    private static final Set<String> ALLOWED_EXTERNAL_PRODUCER_INVOCATIONS = new HashSet<>(Arrays.asList(
        "flush",
        "metrics",
        "partitionsFor"
    ));

    private final Producer<K, V> producer;

    private final Producer<K, V> producerExternalProxy;

    private final ProducerListener producerListener;

    private final Duration closeTimeout;

    public SendingProducer(KafkaSenderOptions<K, V> options) {
        this.producer = options.createProducer();
        this.producerExternalProxy = Proxying.interfaceMethods(Producer.class, this::invokeProducerFromExternal);
        this.producerListener = options.createProducerListener(this);
        this.closeTimeout = options.closeTimeout();
    }

    @Override
    public <T> Mono<T> invokeAndGet(Function<? super Producer<?, ?>, T> invocation) {
        return Mono.fromCallable(() -> invocation.apply(producerExternalProxy));
    }

    public void send(ProducerRecord<K, V> producerRecord, Callback callback) {
        producer.send(producerRecord, callback);
    }

    public void close() {
        runSafely(() -> producerListener.onClose(producerExternalProxy), "producerListener::onClose");
        producer.close(closeTimeout);
    }

    private Object invokeProducerFromExternal(Method method, Object[] args) throws ReflectiveOperationException {
        if (!ALLOWED_EXTERNAL_PRODUCER_INVOCATIONS.contains(method.getName())) {
            throw new UnsupportedOperationException("Kafka Producer method is not supported: " + method);
        }
        return method.invoke(producer, args);
    }

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }
}
