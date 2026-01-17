package io.atleon.core;

import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link AloStream} that is configured by its own self. Extending this is a handy shortcut for
 * implementing streams in applications/contexts where it's possible to inject resources directly
 * into the stream while also having the lifespan of the stream be managed by the containing
 * application/context (like Spring).
 */
public abstract class SelfConfigurableAloStream extends AloStream<SelfConfigurableAloStream>
        implements AloStreamConfig {

    private final AtomicReference<Integer> instanceId = new AtomicReference<>();

    @Override
    public String name() {
        return instanceId().map(id -> AloStreamConfig.super.name() + "-i" + id).orElseGet(AloStreamConfig.super::name);
    }

    @Override
    protected final Disposable startDisposable(SelfConfigurableAloStream self) {
        return startDisposable();
    }

    protected abstract Disposable startDisposable();

    protected Scheduler newBoundedElasticScheduler(int threadCap) {
        return newBoundedElasticScheduler(name(), threadCap);
    }

    protected final Optional<Integer> instanceId() {
        return Optional.ofNullable(instanceId.get());
    }

    SelfConfigurableAloStream withInstanceId(int instanceId) {
        this.instanceId.set(instanceId);
        return this;
    }
}
