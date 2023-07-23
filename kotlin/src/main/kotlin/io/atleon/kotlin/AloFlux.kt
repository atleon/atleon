package io.atleon.kotlin

import io.atleon.core.AloFlux
import io.atleon.util.Defaults
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.publish
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.reactor.mono
import org.reactivestreams.Publisher

/**
 * Apply a coroutine-based mapping of items to an [AloFlux].
 */
inline fun <T, reified V> AloFlux<T>.suspendMap(noinline mapper: suspend (T) -> V): AloFlux<V> = suspendMap(mapper, 1)

/**
 * Apply a coroutine-based mapping of items to an [AloFlux] with a configurable maximum number of
 * concurrent invocations. Concurrent outputs are interleaved and therefore emission order is
 * non-deterministic.
 */
inline fun <T, reified V> AloFlux<T>.suspendMap(noinline mapper: suspend (T) -> V, concurrency: Int): AloFlux<V> {
    val publishingMapper: (T) -> Publisher<V> =
        if (V::class != Unit::class) {
            { mono { mapper.invoke(it) } }
        } else {
            { publish { mapper.invoke(it) } }
        }
    return if (concurrency == 1) concatMap(publishingMapper, 1) else flatMap(publishingMapper, concurrency, 1)
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
    val publishingMapper: (T) -> Publisher<V> = { mapper.invoke(it).asFlux() }
    return if (concurrency == 1) concatMap(publishingMapper, prefetch) else flatMap(publishingMapper, concurrency, prefetch)
}