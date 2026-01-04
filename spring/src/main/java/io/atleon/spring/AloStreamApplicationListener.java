package io.atleon.spring;

import io.atleon.application.AbstractConfiguredAloStream;
import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;

public class AloStreamApplicationListener<C extends AloStreamConfig> extends AbstractConfiguredAloStream<C>
        implements ApplicationListener<ApplicationContextEvent> {

    public AloStreamApplicationListener(AloStream<C> stream, C config) {
        super(stream, config);
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            applicationReady(isAutoStartEnabled(event.getApplicationContext(), name()));
        } else if (event instanceof ContextClosedEvent) {
            stop();
        }
    }

    private static boolean isAutoStartEnabled(ApplicationContext context, String streamName) {
        return !Contexts.isPropertySetToFalse(context, "atleon.stream." + streamName + ".auto.start.enabled");
    }
}
