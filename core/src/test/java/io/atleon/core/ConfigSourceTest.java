package io.atleon.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigSourceTest {

    private static final String CLIENT_ID = "CLIENT_ID";

    private static final String PROPERTY = "PROPERTY";

    private static final String ADDED_PROPERTY = "ADDED_PROPERTY";

    private DummyConfigSource configSource;

    @BeforeEach
    public void setup() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("client.id", CLIENT_ID);
        properties.put(PROPERTY, "ORIGINAL_VALUE");
        configSource = DummyConfigSource.named(CLIENT_ID).withAll(properties);
    }

    @AfterEach
    public void teardown() {
        Set<String> keysToRemove = System.getProperties().keySet().stream()
            .map(Objects::toString)
            .filter(key -> key.startsWith(EnvironmentalConfigs.PREFIX))
            .collect(Collectors.toSet());
        keysToRemove.forEach(System.getProperties()::remove);
        TestConfigProcessor.resetCount();
    }

    @Test
    public void configFactoryCreatesCorrectly() {
        Map<String, Object> result = configSource.create().block();

        assertEquals(2, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertiesCanBeOverridden() {
        System.setProperty(EnvironmentalConfigs.PREFIX + CLIENT_ID + "." + PROPERTY, "SUPER_NEW_VALUE");

        Map<String, Object> result = configSource.create().block();

        assertEquals(2, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("SUPER_NEW_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertiesCanBeAddedThroughSpecificOverrides() {
        System.setProperty(EnvironmentalConfigs.PREFIX + CLIENT_ID + "." + ADDED_PROPERTY, "ADDED");

        Map<String, Object> result = configSource.create().block();

        assertEquals(3, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals("ADDED", result.get(ADDED_PROPERTY));
    }

    @Test
    public void propertiesCanBeRandomized() {
        Map<String, Object> result = configSource
            .with("client.id" + ConditionallyRandomizedConfigs.PROPERTY_SUFFIX, true)
            .create().block();

        assertEquals(2, result.size());
        assertTrue(Objects.toString(result.get("client.id")).startsWith(CLIENT_ID));
        assertNotEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertyProcessorsCanBeConfiguredAsList() {
        Map<String, Object> result = configSource
            .with(ConfigSource.PROCESSORS_PROPERTY, Arrays.asList(TestConfigProcessor.class.getName(), TestConfigProcessor.class.getName()))
            .create().block();

        assertEquals(5, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 1), TestConfigProcessor.TEST_VALUE + 1);
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 2), TestConfigProcessor.TEST_VALUE + 2);
    }

    @Test
    public void propertyProcessorsCanBeConfiguredAsCommaSeparatedList() {
        Map<String, Object> result = configSource
            .with(ConfigSource.PROCESSORS_PROPERTY, String.format("%s,%s", TestConfigProcessor.class.getName(), TestConfigProcessor.class.getName()))
            .create().block();

        assertEquals(5, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 1), TestConfigProcessor.TEST_VALUE + 1);
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 2), TestConfigProcessor.TEST_VALUE + 2);
    }

    private static final class DummyConfigSource extends ConfigSource<Map<String, Object>, DummyConfigSource> {

        private DummyConfigSource() {

        }

        private DummyConfigSource(String name) {
            super(name);
        }

        public static DummyConfigSource named(String name) {
            return new DummyConfigSource(name);
        }

        @Override
        protected DummyConfigSource initializeSourceCopy() {
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