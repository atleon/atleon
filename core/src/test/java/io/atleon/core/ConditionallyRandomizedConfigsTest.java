package io.atleon.core;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionallyRandomizedConfigsTest {

    @Test
    public void propertiesCanBeRandomized() {
        String propertyKey = "property";
        String propertyValue = "value";
        String clientIdKey = "client.id";
        String clientIdValue = "client";

        Map<String, Object> result = new DummyConfigSource()
            .with(propertyKey, propertyValue)
            .with(clientIdKey, clientIdValue)
            .with(clientIdKey + ConditionallyRandomizedConfigs.PROPERTY_SUFFIX, true)
            .create()
            .block();

        assertEquals(2, result.size());
        assertTrue(Objects.toString(result.get(clientIdKey)).startsWith(clientIdValue));
        assertNotEquals(clientIdValue, result.get(clientIdKey));
        assertEquals(propertyValue, result.get(propertyKey));
    }

    private static final class DummyConfigSource extends ConfigSource<Map<String, Object>, DummyConfigSource> {

        @Override
        protected DummyConfigSource initializeCopy() {
            return new DummyConfigSource();
        }

        @Override
        protected void validateProperties(Map<String, Object> properties) {

        }

        @Override
        protected Map<String, Object> postProcessProperties(Map<String, Object> properties) {
            return properties;
        }
    }
}