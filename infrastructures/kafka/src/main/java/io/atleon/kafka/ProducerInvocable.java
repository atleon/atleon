package io.atleon.kafka;

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;
import reactor.core.publisher.Mono;

/**
 * A facade around an active {@link Producer} instance that allows safely invoking (allowed)
 * methods.
 */
public interface ProducerInvocable {

    default Mono<Void> invoke(Consumer<Producer<?, ?>> invocation) {
        return invokeAndGet(producer -> {
            invocation.accept(producer);
            return null;
        });
    }

    <T> Mono<T> invokeAndGet(Function<? super Producer<?, ?>, T> invocation);
}
