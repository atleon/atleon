package io.atleon.application;

import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Utility methods for managing compatibilities between streams and configs
 */
public final class AloStreamCompatibility {

    private AloStreamCompatibility() {}

    public static <C extends AloStreamConfig> Optional<AloStream<? super C>> findSingleCompatibleStream(
            Collection<? extends AloStream<?>> registeredStreams, C config) {
        List<? extends AloStream<? super C>> compatibleStreams = registeredStreams.stream()
                .filter(stream -> isCompatible((Class<? extends AloStream<?>>) stream.getClass(), config))
                .map(stream -> (AloStream<? super C>) stream)
                .collect(Collectors.toList());

        if (compatibleStreams.isEmpty()) {
            return Optional.empty();
        } else if (compatibleStreams.size() == 1) {
            return Optional.of(compatibleStreams.get(0));
        } else {
            throw new IllegalStateException(
                    "There is more than one registered stream compatible with config=" + config);
        }
    }

    public static boolean isCompatible(Class<? extends AloStream<?>> streamType, AloStreamConfig config) {
        return deduceConfigType(streamType).isAssignableFrom(config.getClass());
    }

    private static Class<?> deduceConfigType(Class<? extends AloStream<?>> streamType) {
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
}
