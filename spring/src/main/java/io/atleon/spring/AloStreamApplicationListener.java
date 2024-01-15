package io.atleon.spring;

import io.atleon.application.ConfiguredAloStream;
import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;

public class AloStreamApplicationListener<C extends AloStreamConfig> implements ApplicationListener<ApplicationContextEvent>, ConfiguredAloStream {

    private final AloStream<C> stream;

    private final C config;

    public AloStreamApplicationListener(AloStream<C> stream, C config) {
        this.stream = stream;
        this.config = config;
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent && isAutoStartEnabled(event.getApplicationContext(), name())) {
            start();
        } else if (event instanceof ContextClosedEvent) {
            stop();
        }
    }

    @Override
    public void start() {
        if (stream.state() == AloStream.State.STOPPED) {
            stream.start(config);
        }
    }

    @Override
    public void stop() {
        stream.stop();
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
        return "AloStreamApplicationListener{stream=" + stream + ", config=" + config + '}';
    }

    private static boolean isAutoStartEnabled(ApplicationContext context, String streamName) {
        return !Contexts.isPropertySetToFalse(context, "atleon.stream." + streamName + ".auto.start.enabled");
    }
}
