package io.atleon.protobuf;

import com.google.protobuf.Message;
import io.atleon.util.ConfigLoading;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Function;

public final class ProtobufMessages {

    private ProtobufMessages() {

    }

    public static <I, M extends Message> Function<I, M> loadParser(Map<String, ?> configs, String key, Class<I> inputType) {
        return extractParser(ConfigLoading.loadClassOrThrow(configs, key), inputType);
    }

    private static <I, M extends Message> Function<I, M> extractParser(Class<?> messageType, Class<I> inputType) {
        try {
            Method method = messageType.getDeclaredMethod("parseFrom", inputType);
            return input -> invokeParser(method, input);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                "Either type=" + messageType + " is not a Message or there is no parser for inputType=" + inputType
            );
        }
    }

    private static <M extends Message> M invokeParser(Method parser, Object input) {
        try {
            return (M) parser.invoke(null, input);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Failed to invoke parser=" + parser);
        }
    }
}
