package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

public class AloQueueingTransformerPerformanceTest {

    @Test
    public void processingItemsWithProbabilisticDelayCompletesInReasonableTime() {
        int maxInFlight = 128;
        int parallelism = Runtime.getRuntime().availableProcessors() * 8;
        int count = parallelism * 50;

        AloComponentExtractor<Integer, Integer> componentExtractor = AloComponentExtractor.composed(
            value -> () -> {},
            value -> error -> {},
            Function.identity()
        );

        AloQueueingTransformer<Integer, Integer> queueingTransformer = AloQueueingTransformer.create(componentExtractor)
            .withMaxInFlight(maxInFlight);

        long startEpochMillis = System.currentTimeMillis();
        Flux.range(0, count)
            .transform(queueingTransformer)
            .flatMap(it -> delayProbabilistically(it, .25, 250).doOnSuccess(Alo::acknowledge), parallelism)
            .then()
            .block();

        System.out.printf(
            "Processing count=%d took duration=%6d milliseconds\n",
            count,
            System.currentTimeMillis() - startEpochMillis
        );
    }

    private static <T> Mono<T> delayProbabilistically(T element, double probability, int maxDelayMillis) {
        long delayMillis = Math.random() <= probability ? (long) (Math.random() * maxDelayMillis) : 0L;
        return Mono.just(element).delayElement(Duration.ofMillis(delayMillis));
    }
}
