package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import io.atleon.polling.Polled;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.NonNull;

public class PollingEventLoop<P, O> implements Sinks.EmitFailureHandler {

    private static final Logger log = LoggerFactory.getLogger(PollingEventLoop.class);
    private final Scheduler scheduler;
    private final Pollable<P, O> pollable;
    private final Sinks.Many<Collection<Polled<P, O>>> sink;
    private final PollingEvent pollEvent;
    private final AtomicBoolean active;
    private final Duration pollingInterval;

    public PollingEventLoop(
            final Scheduler scheduler,
            final Pollable<P, O> pollable,
            final Duration pollingInterval,
            final Sinks.Many<Collection<Polled<P, O>>> sink) {
        this.scheduler = scheduler;
        this.pollable = pollable;
        this.pollingInterval = pollingInterval;
        this.sink = sink;
        this.active = new AtomicBoolean(true);
        this.pollEvent = new PollingEvent();
    }

    void onRequest(long toAdd) {
        active.set(true);
        pollEvent.scheduleImmediate();
    }

    public Mono<Void> stop() {
        return Mono.defer(() -> {
                    pollEvent.stop();
                    return Mono.<Void>empty();
                })
                .onErrorResume(e -> Mono.empty());
    }

    @Override
    public boolean onEmitFailure(@NonNull final SignalType signalType, @NonNull final Sinks.EmitResult emitResult) {
        if (!active.get()) {
            return false;
        } else {
            return emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED;
        }
    }

    class PollingEvent implements Runnable {

        private final AtomicBoolean scheduled = new AtomicBoolean(false);

        public void stop() {
            active.compareAndSet(true, false);
        }

        @Override
        public void run() {
            try {
                if (active.get()) {
                    final Collection<Polled<P, O>> result = pollable.poll();

                    if (active.get()) {
                        scheduled.set(false);
                        schedule();
                    }

                    if (result.iterator().hasNext()) {
                        sink.emitNext(result, PollingEventLoop.this);
                    }
                }
            } catch (Exception e) {
                if (active.get()) {
                    log.error("Unexpected exception", e);
                    sink.emitError(e, PollingEventLoop.this);
                }
            }
        }

        void schedule() {
            schedule(pollingInterval.toMillis());
        }

        void scheduleImmediate() {
            schedule(0);
        }

        void schedule(final long interval) {
            if (!scheduled.getAndSet(true)) {
                scheduler.schedule(this, interval, TimeUnit.MILLISECONDS);
            }
        }
    }
}
