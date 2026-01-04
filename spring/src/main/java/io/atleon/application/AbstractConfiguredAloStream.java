package io.atleon.application;

import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import io.atleon.core.Autostart;
import io.atleon.core.StarterStopper;
import io.atleon.core.StarterStopperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * Convenient base implementation of {@link ConfiguredAloStream} used to bootstrap
 * application-specific extensions.
 *
 * @param <C> The type of {@link AloStreamConfig} used to configure a provided {@link AloStream}
 */
public abstract class AbstractConfiguredAloStream<C extends AloStreamConfig> implements ConfiguredAloStream {

    private final AloStream<C> stream;

    private final C config;

    private final StarterStopper starterStopper;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private volatile Disposable startStopDisposable;

    public AbstractConfiguredAloStream(AloStream<C> stream, C config) {
        this.stream = stream;
        this.config = config;
        this.starterStopper = config instanceof StarterStopperConfig
                ? StarterStopperConfig.class.cast(config).buildStarterStopper()
                : null;
    }

    @Override
    public void start() {
        doStartStop(starterStopper == null ? Flux.just(true) : starterStopper.startStop());
    }

    @Override
    public void stop() {
        doStartStop(Flux.just(false));
    }

    @Override
    public String name() {
        return config.name();
    }

    @Override
    public AloStream.State state() {
        return stream.state();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{stream=" + stream + ", config=" + config + '}';
    }

    /**
     * Called by extensions when the application is ready to begin running a stream. If autostart
     * is disabled, then stream startup is bypassed, and the stream lifecycle must be managed
     * manually. If autostart is enabled and no {@link StarterStopper} is configured/available,
     * then the stream will be started immediately. If a  {@link StarterStopper} <i>is</i>
     * available, then it is subscribed to for automated stream starting and stopping.
     */
    protected final void applicationReady() {
        applicationReady(true);
    }

    /**
     * @deprecated Use {@link #applicationReady()} instead
     */
    @Deprecated
    protected final void applicationReady(boolean legacyAutostartEnabled) {
        boolean autostart = config.autostart() == Autostart.ENABLED && legacyAutostartEnabled;
        doStartStop(autostart && starterStopper != null ? starterStopper.startStop() : Flux.just(autostart));
    }

    private void doStartStop(Flux<Boolean> startStop) {
        synchronized (stream) {
            if (startStopDisposable != null && !startStopDisposable.isDisposed()) {
                safelyExecute(startStopDisposable::dispose);
            }
            startStopDisposable =
                    startStop.subscribe(this::doStartStop, this::logStartStopError, this::logStartStopCompletion);
        }
    }

    private void doStartStop(boolean start) {
        synchronized (stream) {
            if (!start) {
                safelyExecute(stream::stop);
            } else if (stream.state() == AloStream.State.STOPPED) {
                safelyExecute(() -> stream.start(config));
            }
        }
    }

    private void safelyExecute(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            logger.warn("Failed to safely execute where stream name={}", name(), e);
        }
    }

    private void logStartStopError(Throwable error) {
        logger.error("Dynamic start-stop has failed where stream name={} and state={}", name(), state(), error);
    }

    private void logStartStopCompletion() {
        logger.debug("Dynamic start-stop is completed where stream name={} and state={}", name(), state());
    }
}
