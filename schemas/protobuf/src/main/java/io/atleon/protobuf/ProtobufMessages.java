package io.atleon.protobuf;

import com.google.protobuf.Message;
import io.atleon.util.ConfigLoading;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class ProtobufMessages {

    private ProtobufMessages() {

    }

    public static <I, M extends Message> Optional<Function<I, M>> loadParser(
        Map<String, ?> configs,
        String key,
        Class<I> inputType
    ) {
        return ConfigLoading.loadClass(configs, key).map(it -> createParser(it, inputType));
    }

    public static <I, M extends Message> Function<I, M> loadParserOrThrow(
        Map<String, ?> configs,
        String key,
        Class<I> inputType
    ) {
        return createParser(ConfigLoading.loadClassOrThrow(configs, key), inputType);
    }

    private static <I, M extends Message> Function<I, M> createParser(Class<?> messageType, Class<I> inputType) {
        try {
            Method method = messageType.getDeclaredMethod("parseFrom", inputType);
            return input -> invoke(method, input);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                "Either type=" + messageType + " is not a Message or there is no parser for inputType=" + inputType
            );
        }
    }

    private static <M extends Message> M invoke(Method method, Object input) {
        try {
            return (M) method.invoke(null, input);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Failed to invoke method=" + method);
        }
    }
}
