package io.atleon.spring;

import io.atleon.application.AloStreamCompatibility;
import io.atleon.application.ConfiguredAloStream;
import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import io.atleon.core.CompositeAloStream;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.util.Parsing;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

@Configuration(proxyBeanMethods = false)
public class AloStreamAutoConfiguration {

    @Bean
    @Conditional(AutoConfigureStreamEnabled.class)
    public List<ConfiguredAloStream> autoConfiguredAloStreams(
            ConfigurableApplicationContext context, List<AloStreamConfig> configs) {
        List<ConfiguredAloStream> configuredAloStreams = new ArrayList<>();
        Set<AloStream<?>> uniqueStreams = Collections.newSetFromMap(new IdentityHashMap<>());
        for (AloStreamConfig config : configs) {
            if (config.getClass().isAnnotationPresent(AutoConfigureStream.class)) {
                AutoConfigureStream annotation = config.getClass().getDeclaredAnnotation(AutoConfigureStream.class);
                AloStream<? super AloStreamConfig> stream = findOrCreateStream(context, config, annotation);
                if (uniqueStreams.add(extractStreamInstanceThatShouldBeUnique(stream))) {
                    AloStreamApplicationListener<?> listener = new AloStreamApplicationListener<>(stream, config);
                    context.addApplicationListener(listener);
                    configuredAloStreams.add(listener);
                } else {
                    throw new IllegalStateException("Applicable to more than one config: stream=" + stream);
                }
            }
        }
        return configuredAloStreams;
    }

    private static <C extends AloStreamConfig> AloStream<? super C> findOrCreateStream(
            ConfigurableApplicationContext context, C config, AutoConfigureStream annotation) {
        Class<? extends AloStream<?>> streamType = (Class<? extends AloStream<?>>) annotation.value();
        Collection<? extends AloStream<?>> registeredStreams = config instanceof SelfConfigurableAloStream
                ? Collections.singletonList((SelfConfigurableAloStream) config)
                : context.getBeansOfType(streamType, false, true).values();
        Optional<AloStream<? super C>> compatibleStream =
                AloStreamCompatibility.findSingleCompatibleStream(registeredStreams, config);
        int count = Contexts.parseValue(context, annotation.instanceCountValue())
                .map(Parsing::toInteger)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Could not parse instance count: " + annotation.instanceCountValue()));

        if (compatibleStream.isPresent()) {
            AloStream<? super C> initial = compatibleStream.get();
            return CompositeAloStream.nCopies(count, initial, newCreator(context, initial.getClass()));
        } else if (streamType.equals(AloStream.class)) {
            throw new IllegalStateException("Could not find compatible stream for config=" + config);
        } else if (AloStreamCompatibility.isCompatible(streamType, config)) {
            return CompositeAloStream.nCopies(count, newCreator(context, streamType));
        } else {
            throw new IllegalStateException("stream=" + streamType + " is incompatible with config=" + config);
        }
    }

    private static AloStream<?> extractStreamInstanceThatShouldBeUnique(AloStream<? super AloStreamConfig> stream) {
        if (stream instanceof CompositeAloStream) {
            CompositeAloStream<?> compositeAloStream = (CompositeAloStream<?>) stream;
            return compositeAloStream.componentStreamCount() > 0 ? compositeAloStream.componentStreamAt(0) : stream;
        } else {
            return stream;
        }
    }

    private static <T> Supplier<T> newCreator(ConfigurableApplicationContext context, Class<?> type) {
        return () -> context.getBeanFactory().createBean((Class<? extends T>) type);
    }

    public static final class AutoConfigureStreamEnabled implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return !Contexts.isPropertySetToFalse(context, "atleon.stream.configure.auto.enabled");
        }
    }
}
