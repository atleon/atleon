package io.atleon.kotlin

import io.atleon.core.AloFlux
import io.atleon.core.GroupFlux
import kotlinx.coroutines.flow.Flow

/**
 * Convenience method for applying [AloFlux.suspendMap] to each inner grouped sequence
 */
inline fun <K, T, reified V> GroupFlux<K, T>.innerSuspendMap(noinline mapper: suspend (T) -> V?): GroupFlux<K, V> =
    map { it.suspendMap(mapper) }

/**
 * Convenience method for applying [AloFlux.suspendMap] with configurable prefetch to each inner
 * grouped sequence
 */
inline fun <K, T, reified V> GroupFlux<K, T>.innerSuspendMapPrefetch(
    noinline mapper: suspend (T) -> V?,
    prefetch: Int
): GroupFlux<K, V> = map { it.suspendMapPrefetch(mapper, prefetch) }

/**
 * Convenience method for applying [AloFlux.flowMap] to each inner grouped sequence
 */
fun <K, T, V : Any> GroupFlux<K, T>.innerFlowMap(mapper: (T) -> Flow<V>): GroupFlux<K, V> =
    map { it.flowMap(mapper) }

/**
 * Convenience method for applying [AloFlux.suspendMap] with configurable prefetch to each inner
 * grouped sequence
 */
fun <K, T, V : Any> GroupFlux<K, T>.innerFlowMapPrefetch(mapper: (T) -> Flow<V>, prefetch: Int): GroupFlux<K, V> =
    map { it.flowMapPrefetch(mapper, prefetch) }