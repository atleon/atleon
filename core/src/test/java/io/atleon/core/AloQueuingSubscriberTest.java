package io.atleon.core;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AloQueuingSubscriberTest {

    protected static final Executor EXECUTOR = Executors.newCachedThreadPool();

    @Test
    public void acknowledgementOnlyEverExecutesOnOneThreadInNonBlockingFashion() throws Exception {
        CompletableFuture<Boolean> firstAcknowledgementStarted = new CompletableFuture<>();
        CompletableFuture<Boolean> firstAcknowledgementAllowed = new CompletableFuture<>();
        TestAlo firstAcknowledgeable = new TestAlo("DATA", () -> {
            try {
                firstAcknowledgementStarted.complete(true);
                firstAcknowledgementAllowed.get();
            } catch (Exception e) {
                fail("Failed to block on second acknowledgement");
            }
        });

        AtomicInteger secondAcknowledgmentPreAllowedCount = new AtomicInteger(0);
        CompletableFuture<Boolean> secondAcknowledgerStarted = new CompletableFuture<>();
        CompletableFuture<Boolean> secondAcknowledgementAllowed = new CompletableFuture<>();
        AtomicInteger secondAcknowledgmentPostAllowedCount = new AtomicInteger(0);
        CompletableFuture<Boolean> secondAcknowledgerFinished = new CompletableFuture<>();
        TestAlo secondAcknowledgeable = new TestAlo("DATA", () -> {
            try {
                secondAcknowledgmentPreAllowedCount.incrementAndGet();
                secondAcknowledgerStarted.complete(true);
                secondAcknowledgementAllowed.get();
                secondAcknowledgmentPostAllowedCount.incrementAndGet();
                secondAcknowledgerFinished.complete(true);
            } catch (Exception e) {
                fail("Failed to block on second acknowledgement");
            }
        });

        List<Alo<String>> emitted = new ArrayList<>();
        Subscriber<Alo<String>> subscriber = Adapters.toSubscriber(emitted::add);

        Sinks.Many<TestAlo> sink = Sinks.many().multicast().onBackpressureBuffer();
        sink.asFlux().subscribe(
            new AloQueueingSubscriber<>(subscriber, String::length, OrderManagingAcknowledgementQueue::create, 2)
        );

        sink.tryEmitNext(firstAcknowledgeable);
        sink.tryEmitNext(secondAcknowledgeable);

        EXECUTOR.execute(() -> Alo.acknowledge(emitted.get(0)));
        assertTrue(firstAcknowledgementStarted.get());

        // Erroneous implementation would block here indefinitely since first acknowledgement is still in-process (blocked)
        Alo.acknowledge(emitted.get(1));

        assertFalse(firstAcknowledgeable.isAcknowledged());
        assertFalse(secondAcknowledgeable.isAcknowledged());
        assertEquals(0, secondAcknowledgmentPreAllowedCount.get());
        assertFalse(secondAcknowledgerStarted.isDone());

        firstAcknowledgementAllowed.complete(true);
        assertTrue(secondAcknowledgerStarted.get());
        assertEquals(1, secondAcknowledgmentPreAllowedCount.get());
        assertTrue(firstAcknowledgeable.isAcknowledged());

        secondAcknowledgementAllowed.complete(true);

        Timing.waitForCondition(secondAcknowledgeable::isAcknowledged);

        assertTrue(secondAcknowledgeable.isAcknowledged());
        assertEquals(1, secondAcknowledgmentPreAllowedCount.get());
        assertEquals(1, secondAcknowledgmentPostAllowedCount.get());
    }

    @Test
    public void acknowledgeableEmissionsAreBoundedInQueuingOrder() {
        TestAlo mom = new TestAlo("MOM");
        TestAlo dad = new TestAlo("DAD");
        TestAlo dog = new TestAlo("DOG");
        TestAlo cat = new TestAlo("CAT");
        TestAlo boy = new TestAlo("BOY");
        Collection<TestAlo> all = Arrays.asList(mom, dad, dog, cat, boy);

        List<Alo<String>> emitted = new ArrayList<>();
        Subscriber<Alo<String>> subscriber = Adapters.toSubscriber(emitted::add);

        Sinks.Many<TestAlo> sink = Sinks.many().multicast().onBackpressureBuffer();
        sink.asFlux().subscribe(
            new AloQueueingSubscriber<>(subscriber, String::length, OrderManagingAcknowledgementQueue::create, 2)
        );

        sink.tryEmitNext(mom);
        sink.tryEmitNext(dad);

        assertTrue(all.stream().noneMatch(TestAlo::isAcknowledged));
        assertTrue(all.stream().noneMatch(TestAlo::isNacknowledged));
        assertEquals(2, emitted.size());

        sink.tryEmitNext(dog);

        assertTrue(all.stream().noneMatch(TestAlo::isAcknowledged));
        assertTrue(all.stream().noneMatch(TestAlo::isNacknowledged));
        assertEquals(2, emitted.size());

        sink.tryEmitNext(cat);
        sink.tryEmitNext(boy);

        assertTrue(all.stream().noneMatch(TestAlo::isAcknowledged));
        assertTrue(all.stream().noneMatch(TestAlo::isNacknowledged));
        assertEquals(2, emitted.size());

        Alo.acknowledge(emitted.get(1));

        assertTrue(all.stream().noneMatch(TestAlo::isAcknowledged));
        assertTrue(all.stream().noneMatch(TestAlo::isNacknowledged));
        assertEquals(2, emitted.size());

        Alo.acknowledge(emitted.get(0));

        assertTrue(mom.isAcknowledged());
        assertTrue(dad.isAcknowledged());
        assertFalse(dog.isAcknowledged());
        assertFalse(cat.isAcknowledged());
        assertFalse(boy.isAcknowledged());
        assertEquals(4, emitted.size());

        Alo.acknowledge(emitted.get(2));

        assertTrue(mom.isAcknowledged());
        assertTrue(dad.isAcknowledged());
        assertTrue(dog.isAcknowledged());
        assertFalse(cat.isAcknowledged());
        assertFalse(boy.isAcknowledged());
        assertEquals(5, emitted.size());
    }

    @Test
    public void acknowledgementIsQueuedOnAPerGroupBasis() {
        TestAlo mom = new TestAlo("MOM");
        TestAlo girl = new TestAlo("GIRL");
        TestAlo dad = new TestAlo("DAD");
        TestAlo yeet = new TestAlo("YEET");
        Collection<TestAlo> all = Arrays.asList(mom, girl, dad, yeet);

        Sinks.Many<TestAlo> sink = Sinks.many().multicast().onBackpressureBuffer();

        List<Alo<String>> emitted = new ArrayList<>();
        Subscriber<Alo<String>> subscriber = Adapters.toSubscriber(emitted::add);

        sink.asFlux().subscribe(
            new AloQueueingSubscriber<>(subscriber, String::length, OrderManagingAcknowledgementQueue::create, 3)
        );

        all.forEach(sink::tryEmitNext);

        assertEquals(3, emitted.size());

        Alo.acknowledge(emitted.get(2));

        assertTrue(all.stream().noneMatch(TestAlo::isAcknowledged));

        Alo.acknowledge(emitted.get(0));

        assertTrue(mom.isAcknowledged());
        assertFalse(girl.isAcknowledged());
        assertTrue(dad.isAcknowledged());
        assertFalse(yeet.isAcknowledged());
        assertEquals(4, emitted.size());

        Alo.acknowledge(emitted.get(1));

        assertTrue(mom.isAcknowledged());
        assertTrue(girl.isAcknowledged());
        assertTrue(dad.isAcknowledged());
        assertFalse(yeet.isAcknowledged());

        Alo.acknowledge(emitted.get(3));

        assertTrue(all.stream().allMatch(TestAlo::isAcknowledged));
    }
}