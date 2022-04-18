package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import io.atleon.polling.Polled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class PollerImp<P, O> implements Poller<P, O> {

    private final Pollable<P, O> pollable;
    private final Scheduler scheduler;
    private final Sinks.Many<Collection<Polled<P, O>>> sink;
    private final PollingEventLoop<P, O> eventLoop;

    protected PollerImp(final Pollable<P, O> pollable,
                        final Duration pollingInterval) {
        this.pollable = pollable;
        this.sink = Sinks.many().unicast().onBackpressureBuffer();
        this.scheduler = Schedulers.newSingle(new EventThreadFactory());
        this.eventLoop = new PollingEventLoop<>(scheduler, pollable, pollingInterval, sink);
        this.scheduler.start();
    }

    @Override
    public Pollable<P, O> getPollable() {
        return pollable;
    }

    @Override
    public Flux<Collection<Polled<P, O>>> receive() {
        return sink.asFlux().doOnRequest(eventLoop::onRequest);
    }

    @Override
    public Mono<Void> close() {
        return eventLoop.stop().doFinally(s -> scheduler.dispose());
    }

    final static class EventThreadFactory implements ThreadFactory {

        static final String PREFIX = "reactive-polling";
        static final AtomicLong COUNTER_REFERENCE = new AtomicLong();

        @Override
        public Thread newThread(Runnable runnable) {
            String newThreadName = PREFIX + "-" + COUNTER_REFERENCE.incrementAndGet();
            Thread t = new EmitterThread(runnable, newThreadName);
            t.setUncaughtExceptionHandler(PollerImp::defaultUncaughtException);
            return t;
        }

        static final class EmitterThread extends Thread {

            EmitterThread(Runnable target, String name) {
                super(target, name);
            }
        }
    }

    static void defaultUncaughtException(Thread t, Throwable e) {
        System.out.println("Polling worker in group " + t.getThreadGroup().getName()
                + " failed with an uncaught exception");
    }

}
