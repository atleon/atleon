package io.atleon.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EnvironmentalConfigsTest {

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

    private static final class DummyConfigSource extends ConfigSource<Map<String, Object>, DummyConfigSource> {

        private DummyConfigSource() {}

        private DummyConfigSource(String name) {
            super(name);
        }

        public static DummyConfigSource named(String name) {
            return new DummyConfigSource(name);
        }

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
