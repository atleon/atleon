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

    private DummyConfigSource configFactory;

    @BeforeEach
    public void setup() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("client.id", CLIENT_ID);
        properties.put(PROPERTY, "ORIGINAL_VALUE");
        configFactory = new DummyConfigSource(CLIENT_ID, properties);
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
        Map<String, Object> result = configFactory.create().block();

        assertEquals(2, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertiesCanBeOverridden() {
        System.setProperty(EnvironmentalConfigs.PREFIX + CLIENT_ID + "." + PROPERTY, "SUPER_NEW_VALUE");

        Map<String, Object> result = configFactory.create().block();

        assertEquals(2, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("SUPER_NEW_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertiesCanBeAddedThroughSpecificOverrides() {
        System.setProperty(EnvironmentalConfigs.PREFIX + CLIENT_ID + "." + ADDED_PROPERTY, "ADDED");

        Map<String, Object> result = configFactory.create().block();

        assertEquals(3, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals("ADDED", result.get(ADDED_PROPERTY));
    }

    @Test
    public void propertiesCanBeRandomized() {
        configFactory.put("client.id" + ConditionallyRandomizedConfigs.PROPERTY_SUFFIX, true);

        Map<String, Object> result = configFactory.create().block();

        assertEquals(2, result.size());
        assertTrue(Objects.toString(result.get("client.id")).startsWith(CLIENT_ID));
        assertNotEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertyProcessorsCanBeConfiguredAsList() {
        configFactory.put(ConfigSource.PROCESSORS_PROPERTY, Arrays.asList(TestConfigProcessor.class.getName(), TestConfigProcessor.class.getName()));

        Map<String, Object> result = configFactory.create().block();

        assertEquals(5, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 1), TestConfigProcessor.TEST_VALUE + 1);
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 2), TestConfigProcessor.TEST_VALUE + 2);
    }

    @Test
    public void propertyProcessorsCanBeConfiguredAsCommaSeparatedList() {
        configFactory.put(ConfigSource.PROCESSORS_PROPERTY, String.format("%s,%s", TestConfigProcessor.class.getName(), TestConfigProcessor.class.getName()));

        Map<String, Object> result = configFactory.create().block();

        assertEquals(5, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 1), TestConfigProcessor.TEST_VALUE + 1);
        assertEquals(result.get(TestConfigProcessor.TEST_KEY + 2), TestConfigProcessor.TEST_VALUE + 2);
    }

    private static final class DummyConfigSource extends ConfigSource<Map<String, Object>, DummyConfigSource> {

        public DummyConfigSource(String name, Map<String, Object> properties) {
            super(name);
            properties.forEach(this::put);
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