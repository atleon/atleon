package io.atleon.polling;

import io.atleon.core.Alo;
import io.atleon.core.AloComponentExtractor;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFactoryConfig;
import io.atleon.core.AloFlux;
import io.atleon.core.AloQueueingTransformer;
import io.atleon.polling.reactive.PollerOptions;
import io.atleon.polling.reactive.PollingReceiver;
import io.atleon.polling.reactive.PollingSourceConfig;
import io.atleon.polling.reactive.ReceiverRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

public class AloPollingReceiver<P, O> {

    /**
     * Strategy for handling Nacknowledgement
     * - EMIT causes error to be emitted to subscribers
     * - NACK causes nacknowledged message to be nack'd with requeue
     * - NACK_EMIT causes nacknowledged message to be nack'ed and the error to be emitted to subscribers
     * Default is EMIT
     */
    public enum NackStrategy {
        EMIT(true, false),
        NACK(false, true),
        NACK_EMIT(true, true);

        private final boolean emit;
        private final boolean nack;

        NackStrategy(final boolean emit,
                     final boolean nack) {
            this.emit = emit;
            this.nack = nack;
        }

        public boolean isEmit() {
            return emit;
        }

        public boolean isNack() {
            return nack;
        }
    }

    private final Pollable<P, O> pollable;
    private final PollingSourceConfig config;
    private final Mono<ReceiveResources<P, O>> resourcesMono;

    private AloPollingReceiver(final Pollable<P, O> pollable,
                               final PollingSourceConfig config) {
        this.pollable = pollable;
        this.config = config;
        this.resourcesMono = Mono.just(ReceiveResources.create(AloFactoryConfig.loadDefault(), config.getNackStrategy()));
    }

    public static <P, O> AloPollingReceiver<P, O> from(final Pollable<P, O> pollable,
                                                       final PollingSourceConfig config) {
        return new AloPollingReceiver<>(pollable, config);
    }

    public AloFlux<P> receivePayloads() {
        return resourcesMono
                .flatMapMany(r -> r.receive(PollingReceiver.create(pollable, buildPollerOptions())))
                .as(AloFlux::wrap)
                .map(Polled::getPayload);
    }

    private PollerOptions buildPollerOptions() {
        return PollerOptions.create(config.getPollingInterval(),
                () -> Schedulers.newSingle(AloPollingReceiver.class.getSimpleName()));
    }

    private static final class ReceiveResources<P, O> {

        private final AloFactory<Polled<P, O>> aloFactory;
        private final NackStrategy nackStrategy;

        private ReceiveResources(final AloFactory<Polled<P, O>> aloFactory,
                                 final NackStrategy nackStrategy) {
            this.aloFactory = aloFactory;
            this.nackStrategy = nackStrategy;
        }

        static <P, O> ReceiveResources<P, O> create(final AloFactory<Polled<P, O>> aloFactory,
                                                    final NackStrategy nackStrategy) {
            return new ReceiveResources<>(aloFactory, nackStrategy);
        }

        public Flux<Alo<Polled<P, O>>> receive(final PollingReceiver<P, O> receiver) {
            final Sinks.Empty<Alo<Polled<P, O>>> sink = Sinks.empty();
            return receiver.receive()
                .transform(newAloQueueingTransformer(sink))
                .mergeWith(sink.asMono());
        }

        private AloQueueingTransformer<ReceiverRecord<P, O>, Polled<P, O>> newAloQueueingTransformer(Sinks.Empty<?> sink) {
            AloComponentExtractor<ReceiverRecord<P, O>, Polled<P, O>> componentExtractor = AloComponentExtractor.composed(
                record -> () -> ack(record),
                record -> error -> nack(sink, error, record),
                ReceiverRecord::getRecord
            );
            return AloQueueingTransformer.create(componentExtractor)
                .withGroupExtractor(receiverRecord -> receiverRecord.getRecord().getGroup());
        }

        private void ack(final ReceiverRecord<P, O> record) {
            record.getPollable().ack(record.getRecord().getOffset());
        }

        private void nack(final Sinks.Empty<?> sink,
                          final Throwable throwable,
                          final ReceiverRecord<P, O> record) {
            if (nackStrategy.nack) {
                record.getPollable().nack(throwable, record.getRecord().getOffset());
            }
            if (nackStrategy.emit) {
                sink.tryEmitError(throwable);
            }
        }
    }
}
