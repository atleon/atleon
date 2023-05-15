package io.atleon.kafka.avro;

import io.atleon.kafka.avro.embedded.EmbeddedSchemaRegistry;
import org.junit.jupiter.api.BeforeEach;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class EmbeddedReflectAvroSerDeTest extends AbstractSerDeTest {

    private static final URL SCHEMA_REGISTRY_URL = EmbeddedSchemaRegistry.startAndGetConnectUrl();

    public EmbeddedReflectAvroSerDeTest() {
        super(new ReflectEncoderAvroSerializer(), new ReflectDecoderAvroDeserializer());
    }

    @BeforeEach
    public void setup() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AvroSerDe.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL.toString());

        serializer.configure(configs, false);
        deserializer.configure(configs, false);
    }
}
