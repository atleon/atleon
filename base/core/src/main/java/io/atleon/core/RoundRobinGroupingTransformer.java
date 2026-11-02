package io.atleon.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

import java.util.function.Function;

/**
 * Assigns elements to integer-keyed groups in round-robin order, starting at zero for each
 * subscription. The sequence of groups completes when the source completes.
 */
final class RoundRobinGroupingTransformer<T> implements Function<Flux<T>, Flux<GroupedFlux<Integer, T>>> {

    private final int cardinality;

    RoundRobinGroupingTransformer(int cardinality) {
        if (cardinality <= 0) {
            throw new IllegalArgumentException("cardinality must be positive");
        }
        this.cardinality = cardinality;
    }

    @Override
    public Flux<GroupedFlux<Integer, T>> apply(Flux<T> flux) {
        return Flux.defer(() -> {
            int[] nextGroup = {0};
            return flux.groupBy(value -> {
                int group = nextGroup[0]++;
                if (nextGroup[0] == cardinality) {
                    nextGroup[0] = 0;
                }
                return group;
            });
        });
    }
}
