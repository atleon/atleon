package io.atleon.context;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.atleon.core.TestAlo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ContextActivatingAloTest {

    private static final AloContext.Key<String> KEY1 = AloContext.Key.single("key1");

    private static final AloContext.Key<String> KEY2 = AloContext.Key.min("key2");

    @Test
    public void contextValuesArePropagatedWithAloTransformations() {
        String value = "value";

        Alo<String> alo = ContextActivatingAlo.create(new TestAlo("test"));

        String result = AloFlux.just(alo)
            .doOnNext(__ -> AloContext.active().set(KEY1, value))
            .map(String::toUpperCase)
            .map(string -> string + ":" + AloContext.active().get(KEY1).orElse(null))
            .consumeAloAndGet(Alo::acknowledge)
            .blockFirst(Duration.ofSeconds(10));

        assertEquals("TEST:value", result);
    }

    @Test
    public void contextValuesAreFannedOutCorrectly() {
        Alo<String> alo = ContextActivatingAlo.create(new TestAlo("test"));

        String result = AloFlux.just(alo)
            .doOnNext(string -> AloContext.active().set(KEY1, string))
            .flatMapIterable(this::extractCharacters)
            .doOnNext(string -> AloContext.active().set(KEY2, string))
            .doOnNext(__ -> assertEquals("test", AloContext.active().get(KEY1).orElse(null)))
            .map(__ -> AloContext.active().get(KEY2).map(String::toUpperCase).orElse(null))
            .consumeAloAndGet(Alo::acknowledge)
            .collect(Collectors.joining(""))
            .block(Duration.ofSeconds(10));

        assertEquals("TEST", result);
    }

    @Test
    public void contextValuesAreFannedInCorrectly() {
        Alo<String> alo = ContextActivatingAlo.create(new TestAlo("test"));

        List<String> result = AloFlux.just(alo)
            .doOnNext(string -> AloContext.active().set(KEY1, string))
            .flatMapIterable(this::extractCharacters)
            .doOnNext(string -> AloContext.active().set(KEY2, string))
            .doOnNext(__ -> assertEquals("test", AloContext.active().get(KEY1).orElse(null)))
            .bufferTimeout(4, Duration.ofSeconds(10))
            .doOnNext(__ -> assertEquals("test", AloContext.active().get(KEY1).orElse(null)))
            .doOnNext(__ -> assertEquals("e", AloContext.active().get(KEY2).orElse(null)))
            .consumeAloAndGet(Alo::acknowledge)
            .blockFirst(Duration.ofSeconds(10));

        assertEquals(Arrays.asList("t", "e", "s", "t"), result);
    }

    private Collection<String> extractCharacters(String string) {
        return IntStream.range(0, string.length())
            .mapToObj(string::charAt)
            .map(Object::toString)
            .collect(Collectors.toList());
    }
}