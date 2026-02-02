package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.atleon.core.SerialQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

final class SendPublisher<T> implements Publisher<RabbitMQSenderResult<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendPublisher.class);

    private final Publisher<? extends RabbitMQSenderMessage<T>> source;

    private final Function<Subscriber<? super RabbitMQSenderResult<T>>, ? extends Send<T>> sender;

    private SendPublisher(
            Publisher<? extends RabbitMQSenderMessage<T>> source,
            Function<Subscriber<? super RabbitMQSenderResult<T>>, ? extends Send<T>> sender) {
        this.source = source;
        this.sender = sender;
    }

    public static <T> SendPublisher<T> immediateError(
            RabbitMQSenderOptions options,
            ReactiveChannel channel,
            Publisher<RabbitMQSenderMessage<T>> senderMessages) {
        return new SendPublisher<>(senderMessages, it -> new ConditionalSend<>(options, channel, it, false));
    }

    public static <T> SendPublisher<T> delegateError(
            RabbitMQSenderOptions options,
            ReactiveChannel channel,
            Publisher<RabbitMQSenderMessage<T>> senderMessages) {
        return new SendPublisher<>(senderMessages, it -> new ConditionalSend<>(options, channel, it, true));
    }

    @Override
    public void subscribe(Subscriber<? super RabbitMQSenderResult<T>> actual) {
        source.subscribe(sender.apply(actual));
    }

    private static void runSafely(Runnable task, String name) {
        try {
            task.run();
        } catch (Exception e) {
            LOGGER.error("Unexpected failure: name={}", name, e);
        }
    }

    private abstract static class Send<T>
            implements CoreSubscriber<RabbitMQSenderMessage<T>>,
                    Subscription,
                    ConfirmListener,
                    ReturnListener,
                    ShutdownListener {

        private enum State {
            ACTIVE,
            TERMINABLE,
            TERMINATED
        }

        private static final AtomicLongFieldUpdater<Send> IN_FLIGHT =
                AtomicLongFieldUpdater.newUpdater(Send.class, "inFlight");

        private static final AtomicLongFieldUpdater<Send> OUTSTANDING_DOWNSTREAM_REQUEST =
                AtomicLongFieldUpdater.newUpdater(Send.class, "outstandingDownstreamRequest");

        private static final AtomicLongFieldUpdater<Send> OUTSTANDING_UPSTREAM_REQUEST =
                AtomicLongFieldUpdater.newUpdater(Send.class, "outstandingUpstreamRequest");

        private static final AtomicReferenceFieldUpdater<Send, State> SUBSCRIPTION_STATE =
                AtomicReferenceFieldUpdater.newUpdater(Send.class, State.class, "subscriptionState");

        private static final AtomicIntegerFieldUpdater<Send> SUBSCRIPTION_DRAINS_IN_PROGRESS =
                AtomicIntegerFieldUpdater.newUpdater(Send.class, "subscriptionDrainsInProgress");

        private final RabbitMQSenderOptions options;

        private final ReactiveChannel channel;

        private final Context subscriberContext;

        private final SerialQueue<Consumer<Subscriber<? super RabbitMQSenderResult<T>>>> emissionQueue;

        private final NavigableMap<Long, OutboundDelivery<T>> inFlightPublishes = new ConcurrentSkipListMap<>();

        private Subscription parent;

        // This counter doubles as both our publishing state (via polarity: positive == ACTIVE,
        // negative == TERMINABLE, zero == TERMINATED) and our count of in-flight sent records (via
        // magnitude: subtract 1 if positive, negate if negative). The extra/initializing count of
        // 1 is in reserve for termination of this subscriber (self). As such, when this becomes
        // non-positive, it means a terminating signal has been received from upstream OR a fatal
        // error has been encountered on sending. When it becomes zero, it means termination has
        // been enqueued to the downstream subscriber.
        private volatile long inFlight = 1;

        private volatile long outstandingDownstreamRequest = 0;

        private volatile long outstandingUpstreamRequest = 0;

        private volatile State subscriptionState = State.ACTIVE;

        private volatile int subscriptionDrainsInProgress = 0;

        protected Send(
                RabbitMQSenderOptions options,
                ReactiveChannel channel,
                CoreSubscriber<? super RabbitMQSenderResult<T>> actual) {
            this.options = options;
            this.channel = channel;
            this.subscriberContext = actual.currentContext();
            this.emissionQueue = SerialQueue.on(actual);

            channel.addPublishListener(this);
        }

        @Override
        public Context currentContext() {
            return subscriberContext;
        }

        @Override
        public void onSubscribe(Subscription s) {
            parent = s;
            emissionQueue.addAndDrain(subscriber -> subscriber.onSubscribe(this));
        }

        @Override
        public void onNext(RabbitMQSenderMessage<T> message) {
            if (IN_FLIGHT.getAndUpdate(this, it -> it > 0 ? it + 1 : it) <= 0) {
                return;
            }

            T correlationMetadata = message.correlationMetadata();
            Consumer<Runnable> runner = options.createCorrelationRunner(correlationMetadata);

            Mono<Void> publish = channel.basicPublish(runner, deliveryTag -> {
                BasicPublishArguments args = new BasicPublishArguments(
                        message.exchange(), message.routingKey(), message.properties(), message.body());

                inFlightPublishes.put(deliveryTag, args.toOutboundDelivery(correlationMetadata));

                return args;
            });

            publish.subscribe(__ -> {}, error -> {
                if (error instanceof PublishException) {
                    PublishException publishException = (PublishException) error;
                    handleOutboundFailure(publishException.deliveryTag(), false, publishException.getCause());
                } else {
                    handleFatalPublishingFailure(error);
                }
            });
        }

        @Override
        public void onError(Throwable t) {
            if (IN_FLIGHT.getAndSet(this, 0) != 0) {
                enqueueTermination(subscriber -> subscriber.onError(t));
            }
        }

        @Override
        public void onComplete() {
            if (IN_FLIGHT.getAndUpdate(this, it -> it > 0 ? 1 - it : it) == 1) {
                enqueueTermination(Subscriber::onComplete);
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                Operators.addCap(OUTSTANDING_DOWNSTREAM_REQUEST, this, n);
                drainSubscription();
            }
        }

        @Override
        public void cancel() {
            if (SUBSCRIPTION_STATE.compareAndSet(this, State.ACTIVE, State.TERMINABLE)) {
                drainSubscription();
            }
        }

        @Override
        public void handleAck(long deliveryTag, boolean multiple) {
            enqueueNextFromOutbound(deliveryTag, multiple, RabbitMQSenderResult::success);
        }

        @Override
        public void handleNack(long deliveryTag, boolean multiple) {
            handleOutboundFailure(deliveryTag, multiple, new NackedOutboundDeliveryException(deliveryTag, multiple));
        }

        @Override
        public void handleReturn(
                int replyCode,
                String replyText,
                String exchange,
                String routingKey,
                AMQP.BasicProperties properties,
                byte[] body) {
            handleFatalPublishingFailure(new ReturnedOutboundDeliveryException(replyCode, replyText));
        }

        @Override
        public void shutdownCompleted(ShutdownSignalException cause) {
            handleFatalPublishingFailure(cause);
        }

        protected final void handleOutboundFailure(long deliveryTag, boolean multiple, Throwable error) {
            if (shouldEmitFailureAsResult()) {
                enqueueNextFromOutbound(deliveryTag, multiple, it -> RabbitMQSenderResult.failure(it, error));
            } else {
                handleFatalPublishingFailure(error);
            }
        }

        protected abstract boolean shouldEmitFailureAsResult();

        private void enqueueNextFromOutbound(
                long deliveryTag,
                boolean multiple,
                Function<OutboundDelivery<T>, RabbitMQSenderResult<T>> senderResultFactory) {
            NavigableMap<Long, OutboundDelivery<T>> deliveries = multiple
                    ? inFlightPublishes.headMap(deliveryTag, true)
                    : inFlightPublishes.subMap(deliveryTag, true, deliveryTag, true);

            Iterator<OutboundDelivery<T>> iterator = deliveries.values().iterator();
            while (iterator.hasNext()) {
                RabbitMQSenderResult<T> senderResult = senderResultFactory.apply(iterator.next());
                enqueueNext(senderResult);
                iterator.remove();
            }
        }

        private void enqueueNext(RabbitMQSenderResult<T> result) {
            emissionQueue.addAndDrain(subscriber -> {
                if (inFlight == 0 || subscriptionState != State.ACTIVE) {
                    return;
                }

                try {
                    subscriber.onNext(result);
                } catch (Throwable error) {
                    LOGGER.error("Emission failure (violates Reactive Streams §2.13)", error);
                    handleFatalPublishingFailure(error);
                    return;
                }

                long previousInFlight = IN_FLIGHT.getAndUpdate(this, count -> {
                    if (count > 0) {
                        return count - 1;
                    } else if (count == 0) {
                        return count;
                    } else {
                        return count + 1;
                    }
                });
                if (outstandingUpstreamRequest != Long.MAX_VALUE) {
                    OUTSTANDING_UPSTREAM_REQUEST.decrementAndGet(this);
                }

                // If the previous in-flight count was positive, then we are not yet eligible for
                // termination, and we should ensure that any capacity freed by emitting the last
                // result is reflected by upstream demand. Only if the previous count was exactly
                // -1 do we know that we are eligible for termination, and that this was the last
                // in-flight result to emit, so we can emit termination. Note that because onError
                // and onComplete immediately make the in-flight count non-positive, it will never
                // be those calling threads that could also invoke drainSubscription (which would
                // be a violation of §2.3).
                if (previousInFlight > 0) {
                    drainSubscription();
                } else if (previousInFlight == -1 && subscriptionState == State.ACTIVE) {
                    enqueueTermination(Subscriber::onComplete);
                }
            });
        }

        private void enqueueTermination(Consumer<Subscriber<?>> terminator) {
            // This is only ever invoked at-most-once after termination has been signaled from
            // upstream AND inFlight has reached zero. This could race with cancellation, but it's
            // not a spec violation if upstream termination is concurrent with downstream cancel.
            enqueueDetachment(subscriber -> runSafely(() -> terminator.accept(subscriber), "Termination"));
        }

        private void handleFatalPublishingFailure(Throwable error) {
            cancel();
            onError(error);
        }

        private void drainSubscription() {
            if (subscriptionState == State.TERMINATED || SUBSCRIPTION_DRAINS_IN_PROGRESS.getAndIncrement(this) != 0) {
                return;
            }

            int missed = 1;
            do {
                if (subscriptionState == State.ACTIVE) {
                    long toRequest = calculateMaxUpstreamRequest();

                    if (toRequest > 0 && outstandingUpstreamRequest != Long.MAX_VALUE) {
                        if (outstandingDownstreamRequest != Long.MAX_VALUE) {
                            OUTSTANDING_DOWNSTREAM_REQUEST.addAndGet(this, -toRequest);
                        }
                        Operators.addCap(OUTSTANDING_UPSTREAM_REQUEST, this, toRequest);
                        runSafely(() -> parent.request(toRequest), "parent::request");
                    }
                } else if (SUBSCRIPTION_STATE.compareAndSet(this, State.TERMINABLE, State.TERMINATED)) {
                    runSafely(parent::cancel, "parent::cancel");
                    enqueueDetachment(__ -> {});
                }

                missed = SUBSCRIPTION_DRAINS_IN_PROGRESS.addAndGet(this, -missed);
            } while (missed != 0);
        }

        private long calculateMaxUpstreamRequest() {
            if (options.maxInFlight() == Integer.MAX_VALUE) {
                return outstandingDownstreamRequest;
            } else {
                return Math.min(options.maxInFlight() - outstandingUpstreamRequest, outstandingDownstreamRequest);
            }
        }

        private void enqueueDetachment(Consumer<Subscriber<?>> andThen) {
            emissionQueue.addAndDrain(subscriber -> {
                runSafely(() -> channel.removePublishListener(this), "channel::removePublishListener");
                andThen.accept(subscriber);
            });
        }
    }

    private static final class ConditionalSend<T> extends Send<T> {

        private final boolean emitFailuresAsResults;

        public ConditionalSend(
                RabbitMQSenderOptions options,
                ReactiveChannel channel,
                Subscriber<? super RabbitMQSenderResult<T>> actual,
                boolean emitFailuresAsResults) {
            super(options, channel, Operators.toCoreSubscriber(actual));
            this.emitFailuresAsResults = emitFailuresAsResults;
        }

        @Override
        protected boolean shouldEmitFailureAsResult() {
            return emitFailuresAsResults;
        }
    }
}
