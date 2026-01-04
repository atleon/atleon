package io.atleon.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.StringValue;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class ProtobufMessagesTest {

    @Test
    public void protobufMessageParsersCanBeLoaded() {
        StringValue message = StringValue.newBuilder().setValue("test").build();

        Map<String, ?> configs = Collections.singletonMap("type", StringValue.class);

        Function<byte[], StringValue> parser = ProtobufMessages.loadParserOrThrow(configs, "type", byte[].class);

        assertEquals(message, parser.apply(message.toByteArray()));
    }
}
