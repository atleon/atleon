package io.atleon.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultipleAcknowledgementQueueTest {

    @Test
    public void acknowledgementAppliesUpToAndIncludingAllPreviousInFlightAcknowledgements() {
        AcknowledgementQueue queue = new MultipleAcknowledgementQueue();

        AtomicBoolean firstAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> firstNacknowledged = new AtomicReference<>();
        AtomicBoolean secondAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> secondNacknowledged = new AtomicReference<>();
        AtomicBoolean thirdAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> thirdNacknowledged = new AtomicReference<>();
        AtomicBoolean fourthAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> fourthNacknowledged = new AtomicReference<>();
        AtomicBoolean fifthAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> fifthNacknowledged = new AtomicReference<>();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(() -> firstAcknowledged.set(true), firstNacknowledged::set);
        AcknowledgementQueue.InFlight secondInFlight = queue.add(() -> secondAcknowledged.set(true), secondNacknowledged::set);
        AcknowledgementQueue.InFlight thirdInFlight = queue.add(() -> thirdAcknowledged.set(true), thirdNacknowledged::set);
        queue.add(() -> fourthAcknowledged.set(true), fourthNacknowledged::set);
        AcknowledgementQueue.InFlight fifthInFlight = queue.add(() -> fifthAcknowledged.set(true), fifthNacknowledged::set);

        long drained = queue.complete(firstInFlight);

        assertEquals(1L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertNull(secondNacknowledged.get());
        assertFalse(thirdAcknowledged.get());
        assertNull(thirdNacknowledged.get());
        assertFalse(fourthAcknowledged.get());
        assertNull(fourthNacknowledged.get());
        assertFalse(fifthAcknowledged.get());
        assertNull(fifthNacknowledged.get());

        drained = queue.completeExceptionally(thirdInFlight, new IllegalStateException());

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
        assertFalse(thirdAcknowledged.get());
        assertTrue(thirdNacknowledged.get() instanceof IllegalStateException);
        assertFalse(fourthAcknowledged.get());
        assertNull(fourthNacknowledged.get());
        assertFalse(fifthAcknowledged.get());
        assertNull(fifthNacknowledged.get());

        // Duplicate completion should be ignored
        drained = queue.complete(secondInFlight) + queue.completeExceptionally(thirdInFlight, new IllegalArgumentException());

        assertEquals(0L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
        assertFalse(thirdAcknowledged.get());
        assertTrue(thirdNacknowledged.get() instanceof IllegalStateException);
        assertFalse(fourthAcknowledged.get());
        assertNull(fourthNacknowledged.get());
        assertFalse(fifthAcknowledged.get());
        assertNull(fifthNacknowledged.get());

        drained = queue.complete(fifthInFlight);

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
        assertFalse(thirdAcknowledged.get());
        assertTrue(thirdNacknowledged.get() instanceof IllegalStateException);
        assertTrue(fourthAcknowledged.get());
        assertNull(fourthNacknowledged.get());
        assertTrue(fifthAcknowledged.get());
        assertNull(fifthNacknowledged.get());
    }
}