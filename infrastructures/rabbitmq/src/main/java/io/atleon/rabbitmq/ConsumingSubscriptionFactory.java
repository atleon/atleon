package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

final class ConsumingSubscriptionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumingSubscriptionFactory.class);

    private final RabbitMQReceiverOptions options;

    public ConsumingSubscriptionFactory(RabbitMQReceiverOptions options) {
        this.options = options;
    }

    public Subscription create(String queue, Subscriber<? super RabbitMQReceiverMessage> subscriber) {
        return new SubscriptionConsumer(queue, subscriber);
    }

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    private final class SubscriptionConsumer implements Subscription, Consumer {

        private final Mono<ReceivingConnection> connection;

        private final String queue;

        private final Subscriber<? super RabbitMQReceiverMessage> subscriber;

        private final AtomicReference<State> publishingState = new AtomicReference<>(State.ACTIVE);

        // This counter doubles as both our connection state (via polarity: negative == CLOSED,
        // non-negative == OPEN) and (when non-negative) our outstanding request count. As such,
        // it is initialized as (barely) negative to indicate "not yet opened". When this first
        // becomes non-negative, it means connection initialization and consumption initialization
        // have (at least) been scheduled (or about to be). When it becomes negative again, it
        // means the connection has been (at least) scheduled to be closed.
        private final AtomicLong requested = new AtomicLong(-1L);

        private final Queue<RabbitMQReceiverMessage> emittableMessages = new ConcurrentLinkedQueue<>();

        private final AtomicReference<Throwable> error = new AtomicReference<>();

        private final AtomicInteger drainsInProgress = new AtomicInteger(0);

        public SubscriptionConsumer(String queue, Subscriber<? super RabbitMQReceiverMessage> subscriber) {
            this.connection = ReceivingConnection.create(options).cache();
            this.queue = queue;
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (!Operators.validate(n)) {
                return;
            }

            long previousRequested = requested.getAndUpdate(it -> {
                if (it < 0) {
                    // Either initial request, or request after cancellation.
                    return it == -1 ? n : it;
                } else {
                    return Operators.addCap(it, n);
                }
            });

            if (previousRequested == -1L && publishingState.get() == State.ACTIVE) {
                // Initial request, so initialize consumption. Note that we do not need to also
                // subscribe to connection closure, as the following invocation will notify us of
                // such an error, or will upon/after invoking "consume" via Consumer callback(s).
                scheduleOnConnection(it -> it.consume(queue, this));
            } else if (previousRequested == 0) {
                // Request had been exhausted and could have been limiting emission, so must drain.
                drain();
            }
        }

        @Override
        public void cancel() {
            if (enterTerminableState()) {
                drain();
            }
        }

        @Override
        public void handleConsumeOk(String consumerTag) {
            LOGGER.info("Consume OK with consumerTag={}", consumerTag);
        }

        @Override
        public void handleCancelOk(String consumerTag) {
            LOGGER.info("Cancel OK with consumerTag={}", consumerTag);
        }

        @Override
        public void handleCancel(String consumerTag) {
            failSafely(new CanceledConsumeException(consumerTag));
        }

        @Override
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
            failSafely(sig);
        }

        @Override
        public void handleRecoverOk(String consumerTag) {
            LOGGER.info("Recover OK with consumerTag={}", consumerTag);
        }

        @Override
        public void handleDelivery(
                String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            if (publishingState.get() == State.TERMINATED) {
                return;
            }

            RabbitMQDeliverySettler deliverySettler = new RabbitMQDeliverySettlerImpl(envelope.getDeliveryTag());
            emittableMessages.add(RabbitMQReceiverMessage.create(envelope, properties, body, deliverySettler));
            drain();
        }

        private void failSafely(Throwable failure) {
            if (!active() || !error.compareAndSet(null, failure)) {
                // Failures during termination and failures that don't initiate termination can be
                // safely dropped.
                LOGGER.debug("Ignoring failure during termination", failure);
            } else if (enterTerminableState()) {
                // Could be racing with cancellation, but it's not a spec violation if onError
                // emission is concurrent with downstream cancellation.
                drain();
            }
        }

        private void drain() {
            if (drainsInProgress.getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            do {
                // Handle onNext emission, update outstanding capacities, and re-trigger drain loop
                // if we exhaust whatever resource first limits emission. This makes it such that
                // we don't need to invoke drain every time a possibly-limiting resource is updated
                // (request, active cap, etc.), unless/until it is updated from (or to) zero.
                long maxToEmit = emittableMessages.isEmpty() ? 0L : requested.get();

                if (maxToEmit > 0) {
                    long emitted = 0;
                    RabbitMQReceiverMessage emittable;
                    while (emitted < maxToEmit && active() && (emittable = emittableMessages.poll()) != null) {
                        try {
                            subscriber.onNext(emittable);
                            emitted++;
                        } catch (Throwable error) {
                            LOGGER.error("Emission failure (violates Reactive Streams §2.13)", error);
                            failSafely(error);
                        }
                    }

                    if (requested.get() != Long.MAX_VALUE) {
                        requested.addAndGet(-emitted);
                    }
                    if (maxToEmit == emitted) {
                        drainsInProgress.incrementAndGet();
                    }
                }

                // Handle termination if now is the time to do so. Don't need CAS here since this
                // is the only place to transition to TERMINATED.
                if (publishingState.get() == State.TERMINABLE) {
                    terminateSafely();
                    publishingState.set(State.TERMINATED);
                }

                missed = drainsInProgress.addAndGet(-missed);
            } while (missed != 0);
        }

        private boolean enterTerminableState() {
            return publishingState.compareAndSet(State.ACTIVE, State.TERMINABLE);
        }

        private void terminateSafely() {
            // Check if connection needs to be closed while setting outstanding request to a value
            // that will make subsequent requests no-ops.
            if (requested.getAndSet(Long.MIN_VALUE) >= 0) {
                scheduleOnConnection(ReceivingConnection::closeSafely);
            }

            Throwable errorToEmit = error.get();
            if (errorToEmit != null) {
                runSafely(() -> subscriber.onError(errorToEmit), "subscriber::onError §2.13");
                LOGGER.debug("Terminated due to error");
            } else {
                LOGGER.debug("Terminated due to cancel");
            }
        }

        private void scheduleOnConnection(Function<ReceivingConnection, Mono<?>> function) {
            connection.flatMap(function).subscribe(__ -> {}, this::failSafely);
        }

        private boolean active() {
            return publishingState.get() == State.ACTIVE;
        }

        private final class RabbitMQDeliverySettlerImpl implements RabbitMQDeliverySettler {

            private final long tag;

            private final AtomicBoolean settled = new AtomicBoolean(false);

            public RabbitMQDeliverySettlerImpl(long tag) {
                this.tag = tag;
            }

            @Override
            public void ack() {
                settle(ReceivingConnection::ack);
            }

            @Override
            public void reject() {
                settle(ReceivingConnection::reject);
            }

            @Override
            public void requeue() {
                settle(ReceivingConnection::requeue);
            }

            private void settle(BiFunction<ReceivingConnection, Long, Mono<Void>> settler) {
                if (settled.compareAndSet(false, true)) {
                    scheduleOnConnection(it -> settler.apply(it, tag));
                } else {
                    LOGGER.debug("Delivery already settled where tag={}", tag);
                }
            }
        }
    }

    private enum State {
        ACTIVE,
        TERMINABLE,
        TERMINATED
    }
}
