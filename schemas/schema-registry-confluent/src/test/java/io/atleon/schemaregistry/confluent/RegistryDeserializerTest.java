package io.atleon.schemaregistry.confluent;

import io.atleon.schema.SchematicDeserializer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;

public class RegistryDeserializerTest {

    @Test
    public void invalidAvroDataWithoutSchemaIDCanBeNulledOut() {
        RegistryDeserializer<Object, ?> deserializer = new TestRegistryDeserializer<>();

        Map<String, Object> configs = new HashMap<>();
        configs.put(RegistrySerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "<NOT_USED>");
        configs.put(RegistryDeserializerConfig.READ_NULL_ON_FAILURE_CONFIG, true);

        deserializer.configure(configs);

        assertNull(deserializer.deserialize(new byte[]{0, 1, 2, 3}));
    }

    @Test
    public void nonAvroDataCanBeDeserializedAsNull() {
        RegistryDeserializer<Object, ?> deserializer = new TestRegistryDeserializer<>();

        Map<String, Object> configs = new HashMap<>();
        configs.put(RegistrySerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "<NOT_USED>");
        configs.put(RegistryDeserializerConfig.READ_NULL_ON_FAILURE_CONFIG, true);

        deserializer.configure(configs);

        assertNull(deserializer.deserialize("NON-AVRO".getBytes()));
    }

    private static final class TestRegistryDeserializer<T> extends RegistryDeserializer<T, Object> {

        @Override
        public void configure(Map<String, ?> properties) {
            configure(new RegistryDeserializerConfig(RegistryDeserializerConfig.registryDeserializerConfigDef(), properties));
        }

        @Override
        protected SchematicDeserializer<T, Object> deserializer() {
            return (data, __) -> {
                throw new UnsupportedOperationException();
            };
        }

        @Override
        protected SchemaProvider createSchemaProvider() {
            ClassLoader classLoader = getClass().getClassLoader();
            Class[] interfaces = {SchemaProvider.class};
            InvocationHandler invocationHandler = new TestSchemaProviderInvocationHandler();
            return SchemaProvider.class.cast(Proxy.newProxyInstance(classLoader, interfaces, invocationHandler));
        }

        @Override
        protected Object toSchema(ParsedSchema parsedSchema) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestSchemaProviderInvocationHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "configure":
                    return null;
                case "schemaType":
                    return "TEST";
                default:
                    throw new UnsupportedOperationException("Method not implemented in test: " + method);
            }
        }
    }
}