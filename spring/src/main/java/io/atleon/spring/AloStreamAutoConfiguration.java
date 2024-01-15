package io.atleon.spring;

import io.atleon.application.AloStreamCompatibility;
import io.atleon.application.ConfiguredAloStream;
import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Configuration(proxyBeanMethods = false)
public class AloStreamAutoConfiguration {

    @Bean
    @Conditional(AutoConfigureStreamEnabled.class)
    public List<ConfiguredAloStream>
    autoConfiguredAloStreams(ConfigurableApplicationContext context, List<AloStreamConfig> configs) {
        List<ConfiguredAloStream> configuredAloStreams = new ArrayList<>();
        Set<AloStream<?>> uniqueStreams = Collections.newSetFromMap(new IdentityHashMap<>());
        for (AloStreamConfig config : configs) {
            if (config.getClass().isAnnotationPresent(AutoConfigureStream.class)) {
                AloStream<? super AloStreamConfig> stream = findOrCreateStream(context, config);
                if (uniqueStreams.add(stream)) {
                    AloStreamApplicationListener<?> listener = new AloStreamApplicationListener<>(stream, config);
                    context.addApplicationListener(listener);
                    configuredAloStreams.add(listener);
                } else {
                    throw new IllegalStateException("Instance of stream=" + stream + " applicable to more than one config!");
                }
            }
        }
        return configuredAloStreams;
    }

    private <C extends AloStreamConfig> AloStream<? super C>
    findOrCreateStream(ConfigurableApplicationContext context, C config) {
        AutoConfigureStream annotation = config.getClass().getDeclaredAnnotation(AutoConfigureStream.class);
        Class<? extends AloStream<?>> streamType = (Class<? extends AloStream<?>>) annotation.value();
        Optional<AloStream<? super C>> compatibleStream =
            AloStreamCompatibility.findSingleCompatibleStream(context.getBeansOfType(streamType).values(), config);

        if (compatibleStream.isPresent()) {
            return compatibleStream.get();
        } else if (streamType.equals(AloStream.class)) {
            throw new IllegalStateException("Could not find compatible stream for config=" + config);
        } else if (AloStreamCompatibility.isCompatible(streamType, config)) {
            return context.getBeanFactory().createBean((Class<? extends AloStream<? super C>>) streamType);
        } else {
            throw new IllegalStateException("stream=" + streamType + " is incompatible with config=" + config);
        }
    }

    public static final class AutoConfigureStreamEnabled implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return !Contexts.isPropertySetToFalse(context, "atleon.stream.configure.auto.enabled");
        }
    }
}
