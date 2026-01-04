package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import java.util.function.BiFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class PollingReceiverImp<P, O> implements PollingReceiver<P, O> {

    private final Pollable<P, O> pollable;
    private final PollerOptions pollerOptions;
    private Poller<P, O> poller;

    protected PollingReceiverImp(final Pollable<P, O> pollable, final PollerOptions pollerOptions) {
        this.pollable = pollable;
        this.pollerOptions = pollerOptions;
    }

    @Override
    public Flux<ReceiverRecord<P, O>> receive() {
        return withPoller((scheduler, handler) -> handler.receive()
                .publishOn(scheduler, 1)
                .flatMapIterable(it -> it)
                .map(r -> new ReceiverRecord<>(r, handler.getPollable())));
    }

    private <T> Flux<T> withPoller(BiFunction<Scheduler, Poller<P, O>, Flux<T>> function) {
        return Flux.usingWhen(
                Mono.fromCallable(() -> poller = Poller.create(pollable, pollerOptions.getPollingInterval())),
                poller -> Flux.using(
                        () -> Schedulers.single(
                                pollerOptions.getSchedulerSupplier().get()),
                        scheduler -> function.apply(scheduler, poller),
                        Scheduler::dispose),
                p -> p.close().doFinally(s -> poller = null));
    }
}
