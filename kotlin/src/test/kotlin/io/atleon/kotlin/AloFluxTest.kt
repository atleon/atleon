package io.atleon.kotlin

import io.atleon.core.AloFlux
import io.atleon.core.ComposedAlo
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.util.concurrent.atomic.AtomicBoolean

class AloFluxTest {

    @Test
    fun suspendingTerminalConsumptionCanBeAppliedToAloFlux() {
        val acknowledged = AtomicBoolean(false)
        val nacknowledged = AtomicBoolean(false)
        val alo = ComposedAlo("DATA", { acknowledged.set(true) }, { nacknowledged.set(true) })

        val aloFlux = Flux.just(alo)
            .`as` { AloFlux.wrap(it) }
            .suspendMap { delay(100) }

        StepVerifier.create(aloFlux).expectComplete().verify()

        assertTrue(acknowledged.get())
        assertFalse(nacknowledged.get())
    }

    @Test
    fun suspendingMapperCanBeAppliedToAloFlux() {
        val acknowledged = AtomicBoolean(false)
        val nacknowledged = AtomicBoolean(false)
        val alo = ComposedAlo("DATA", { acknowledged.set(true) }, { nacknowledged.set(true) })

        val aloFlux = Flux.just(alo)
            .`as` { AloFlux.wrap(it) }
            .suspendMap { delay(100).run { it.lowercase() } }

        StepVerifier.create(aloFlux).expectNextMatches { it.get() == "data" }.expectComplete().verify()

        assertFalse(acknowledged.get())
        assertFalse(nacknowledged.get())
    }

    @Test
    fun flowMapperCanBeAppliedToAloFlux() {
        val acknowledged = AtomicBoolean(false)
        val nacknowledged = AtomicBoolean(false)
        val alo = ComposedAlo("DATA", { acknowledged.set(true) }, { nacknowledged.set(true) })

        val aloFlux = Flux.just(alo)
            .`as` { AloFlux.wrap(it) }
            .flowMap { flowOf(it).map(String::lowercase) }

        StepVerifier.create(aloFlux).expectNextMatches { it.get() == "data" }.expectComplete().verify()

        assertFalse(acknowledged.get())
        assertFalse(nacknowledged.get())
    }
}