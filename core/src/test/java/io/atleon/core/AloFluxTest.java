package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AloFluxTest {

    @Test
    public void aloFluxValuesCanBeMappedToNonNullValues() {
        TestAlo empty = new TestAlo("");
        TestAlo nonEmpty = new TestAlo("data");

        List<String> result = new ArrayList<>();
        AloFlux.wrap(Flux.just(empty, nonEmpty))
            .mapNotNull((string) -> string.isEmpty() ? null : string)
            .subscribe((alo) -> result.add(alo.get()));

        assertEquals(Collections.singletonList(nonEmpty.get()), result);
        assertEquals(1, empty.mapCount());
        assertEquals(1, nonEmpty.mapCount());
        assertTrue(empty.isAcknowledged());
        assertFalse(nonEmpty.isAcknowledged());
    }
}