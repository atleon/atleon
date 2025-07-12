package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.function.Function;

/**
 * A facade around an active {@link Consumer} instance that allows safely invoking (allowed)
 * methods from the active polling thread. NOTE: It is invalid to invoke these methods from the
 * polling thread, for example from within invocations of
 * {@link ConsumerListener#onPartitionsAssigned(Consumer, Collection)}. In such cases, it is
 * rather permissible to call methods on the explicitly provided Consumer instance, since the
 * callback is executing on the polling thread anyway.
 */
public interface ConsumerInvocable {

    default Mono<Void> invoke(java.util.function.Consumer<Consumer<?, ?>> invocation) {
        return invokeAndGet(consumer -> {
            invocation.accept(consumer);
            return null;
        });
    }

    <T> Mono<T> invokeAndGet(Function<? super Consumer<?, ?>, T> invocation);
}
