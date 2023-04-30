package io.atleon.kotlin

import io.atleon.core.AloFlux
import io.atleon.util.Defaults
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.publish
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.reactor.mono

/**
 * Apply a terminal coroutine-based consumption to an [AloFlux].
 */
fun <T> AloFlux<T>.suspendConsume(consumer: suspend (T) -> Unit): AloFlux<Void> = suspendConsume(consumer, 1)

/**
 * Apply a terminal coroutine-based consumption to an [AloFlux] with a configurable maximum number
 * of concurrent invocations.
 */
fun <T> AloFlux<T>.suspendConsume(consumer: suspend (T) -> Unit, concurrency: Int): AloFlux<Void> {
    return if (concurrency == 1) {
        concatMap({ publish { consumer.invoke(it) } }, 1)
    } else {
        flatMap({ publish { consumer.invoke(it) } }, concurrency, 1)
    }
}

/**
 * Apply a coroutine-based mapping of items to an [AloFlux].
 */
fun <T, V> AloFlux<T>.suspendMap(mapper: suspend (T) -> V): AloFlux<V> = suspendMap(mapper, 1)

/**
 * Apply a coroutine-based mapping of items to an [AloFlux] with a configurable maximum number of
 * concurrent invocations. Concurrent outputs are interleaved and therefore and therefore emission
 * order is non-deterministic.
 */
fun <T, V> AloFlux<T>.suspendMap(mapper: suspend (T) -> V, concurrency: Int): AloFlux<V> {
    return if (concurrency == 1) {
        concatMap({ mono { mapper.invoke(it) } }, 1)
    } else {
        flatMap({ mono { mapper.invoke(it) } }, concurrency, 1)
    }
}

/**
 * Apply a one-to-many [Flow]-based mapping of items to an [AloFlux].
 */
fun <T, V : Any> AloFlux<T>.flowMap(mapper: (T) -> Flow<V>): AloFlux<V> = flowMap(mapper, 1)

/**
 * Apply a one-to-many [Flow]-based mapping of items to an [AloFlux] with a configurable maximum
 * number of concurrent invocations and in-flight elements from each inner Flow. Concurrent outputs
 * from inner Flows are interleaved and therefore emission order is non-deterministic.
 */
fun <T, V : Any> AloFlux<T>.flowMap(mapper: (T) -> Flow<V>, concurrency: Int, prefetch: Int = Defaults.PREFETCH): AloFlux<V> {
    return if (concurrency == 1) {
        concatMap({ mapper.invoke(it).asFlux() }, prefetch)
    } else {
        flatMap({ mapper.invoke(it).asFlux() }, concurrency, prefetch)
    }
}
