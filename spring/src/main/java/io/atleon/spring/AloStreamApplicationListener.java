package io.atleon.spring;

import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;

public class AloStreamApplicationListener<C extends AloStreamConfig> implements ApplicationListener<ApplicationContextEvent> {

    private final AloStream<C> stream;

    private final C config;

    public AloStreamApplicationListener(AloStream<C> stream, C config) {
        this.stream = stream;
        this.config = config;
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            stream.start(config);
        } else if (event instanceof ContextClosedEvent) {
            stream.stop();
        }
    }
}
