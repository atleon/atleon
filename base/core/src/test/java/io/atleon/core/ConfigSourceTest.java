package io.atleon.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigSourceTest {

    @AfterEach
    public void teardown() {
        TestConfigProcessor.resetCount();
    }

    @Test
    public void configFactoryCreatesCorrectly() {
        String propertyKey = "key";
        String propertyValue = "value";
        String clientIdKey = "client.id";
        String clientIdValue = "client";

        Map<String, Object> result = new DummyConfigSource()
                .with(propertyKey, propertyValue)
                .with(clientIdKey, clientIdValue)
                .create()
                .block();

        assertEquals(2, result.size());
        assertEquals(clientIdValue, result.get(clientIdKey));
        assertEquals(propertyValue, result.get(propertyKey));
    }

    @Test
    public void propertyProcessorsCanBeConfiguredAsList() {
        String propertyKey = "key";
        String propertyValue = "value";
        String clientIdKey = "client.id";
        String clientIdValue = "client";

        List<String> processors =
                Arrays.asList(TestConfigProcessor.class.getName(), TestConfigProcessor.class.getName());
        Map<String, Object> result = new DummyConfigSource()
                .with(propertyKey, propertyValue)
                .with(clientIdKey, clientIdValue)
                .with(ConfigSource.PROCESSORS_PROPERTY, processors)
                .create()
                .block();

        assertEquals(5, result.size());
        assertEquals(clientIdValue, result.get(clientIdKey));
        assertEquals(propertyValue, result.get(propertyKey));
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 1), TestConfigProcessor.TEST_VALUE + 1);
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 2), TestConfigProcessor.TEST_VALUE + 2);
    }

    @Test
    public void propertyProcessorsCanBeConfiguredAsCommaSeparatedList() {
        String propertyKey = "key";
        String propertyValue = "value";
        String clientIdKey = "client.id";
        String clientIdValue = "client";

        List<String> processors =
                Arrays.asList(TestConfigProcessor.class.getName(), TestConfigProcessor.class.getName());
        Map<String, Object> result = new DummyConfigSource()
                .with(propertyKey, propertyValue)
                .with(clientIdKey, clientIdValue)
                .with(ConfigSource.PROCESSORS_PROPERTY, String.join(",", processors))
                .create()
                .block();

        assertEquals(5, result.size());
        assertEquals(clientIdValue, result.get(clientIdKey));
        assertEquals(propertyValue, result.get(propertyKey));
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 1), TestConfigProcessor.TEST_VALUE + 1);
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 2), TestConfigProcessor.TEST_VALUE + 2);
    }

    private static final class DummyConfigSource extends ConfigSource<Map<String, Object>, DummyConfigSource> {

        @Override
        protected DummyConfigSource initializeCopy() {
            return new DummyConfigSource();
        }

        @Override
        protected void validateProperties(Map<String, Object> properties) {}

        @Override
        protected Map<String, Object> postProcessProperties(Map<String, Object> properties) {
            return properties;
        }
    }
}
