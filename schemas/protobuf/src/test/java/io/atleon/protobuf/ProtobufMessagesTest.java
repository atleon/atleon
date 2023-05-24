package io.atleon.protobuf;

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class ProtobufMessagesTest {

    @Test
    public void protobufMessageParsersCanBeLoaded() {
        StringValue message = StringValue.newBuilder().setValue("test").build();

        Map<String, ?> configs = Collections.singletonMap("type", StringValue.class);

        Function<byte[], StringValue> parser = ProtobufMessages.loadParser(configs, "type", byte[].class);

        assertEquals(message, parser.apply(message.toByteArray()));
    }
}