package io.atleon.spring;

import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
        Optional<AloStream<? super C>> compatibleStream = findCompatibleStream(context, config, annotation.value());
        if (compatibleStream.isPresent()) {
            return compatibleStream.get();
        } else if (annotation.value().equals(AloStream.class)) {
            throw new IllegalStateException("Could not find compatible stream for config=" + config);
        } else if (isCompatible(annotation.value(), config)){
            return context.getBeanFactory().createBean(annotation.value());
        } else {
            throw new IllegalStateException("stream=" + annotation.value() + " is incompatible with config=" + config);
        }
    }

    private <C extends AloStreamConfig> Optional<AloStream<? super C>>
    findCompatibleStream(ConfigurableApplicationContext context, C config, Class<? extends AloStream> streamType) {
        List<? extends AloStream> compatibleStreams = context.getBeansOfType(streamType).values().stream()
            .filter(stream -> isCompatible(stream.getClass(), config))
            .collect(Collectors.toList());
        if (compatibleStreams.isEmpty()) {
            return Optional.empty();
        } else if (compatibleStreams.size() == 1) {
            return Optional.of(compatibleStreams.get(0));
        } else {
            throw new IllegalStateException("There is more than one registered stream compatible with config=" + config);
        }
    }

    private static boolean isCompatible(Class<? extends AloStream> streamType, AloStreamConfig config) {
        return deduceConfigType(streamType).isAssignableFrom(config.getClass());
    }

    private static Class<?> deduceConfigType(Class<? extends AloStream> streamType) {
        for (Class<?> type = streamType; type != null; type = type.getSuperclass()) {
            if (type.getSuperclass().equals(AloStream.class)) {
                ParameterizedType aloStreamType = ParameterizedType.class.cast(type.getGenericSuperclass());
                return extractRawType(aloStreamType.getActualTypeArguments()[0]);
            }
        }
        throw new IllegalStateException("Failed to deduce configType for streamType=" + streamType);
    }

    private static Class<?> extractRawType(Type type) {
        if (type instanceof Class) {
            return Class.class.cast(type);
        } else if (type instanceof ParameterizedType) {
            return extractRawType(ParameterizedType.class.cast(type).getRawType());
        } else {
            throw new IllegalArgumentException("Cannot extract raw type from type=" + type);
        }
    }

    public static final class AutoConfigureStreamEnabled implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return !Contexts.isPropertySetToFalse(context, "atleon.stream.configure.auto.enabled");
        }
    }
}
