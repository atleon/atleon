package io.atleon.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.Phaser;
import org.junit.jupiter.api.Test;

class ReactivePhaserTest {

    @Test
    public void reactivePhasingBehavesIdenticallyToBlocking() {
        Phaser phaser = new Phaser(1);
        ReactivePhaser reactivePhaser = new ReactivePhaser(1);

        assertEquals(phaser.arrive(), reactivePhaser.arrive());

        assertEquals(
                phaser.awaitAdvance(0), reactivePhaser.awaitAdvanceReactively(0).block());
        assertEquals(
                phaser.awaitAdvance(0), reactivePhaser.awaitAdvanceReactively(0).block());

        assertEquals(phaser.arrive(), reactivePhaser.arrive());

        assertEquals(
                phaser.awaitAdvance(0), reactivePhaser.awaitAdvanceReactively(0).block());
        assertEquals(
                phaser.awaitAdvance(1), reactivePhaser.awaitAdvanceReactively(1).block());

        phaser.forceTermination();
        reactivePhaser.forceTermination();

        assertEquals(phaser.arrive(), reactivePhaser.arrive());
        assertEquals(
                phaser.awaitAdvance(1), reactivePhaser.awaitAdvanceReactively(1).block());
        assertEquals(
                phaser.awaitAdvance(2), reactivePhaser.awaitAdvanceReactively(2).block());
        assertEquals(
                phaser.awaitAdvance(-1),
                reactivePhaser.awaitAdvanceReactively(-1).block());

        assertEquals(phaser.arrive(), reactivePhaser.arrive());
    }

    @Test
    public void deadlockDoesNotOccurWhenPhaserIsInteractedWithDuringPublishing() {
        ReactivePhaser reactivePhaser = new ReactivePhaser(1);

        Integer arrivalPhase = reactivePhaser
                .arriveAndAwaitAdvanceReactively()
                .doOnNext(__ -> reactivePhaser.register())
                .block();

        assertEquals(0, arrivalPhase);
    }
}
