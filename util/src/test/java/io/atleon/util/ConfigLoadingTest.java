package io.atleon.util;

import org.junit.jupiter.api.Test;

import java.math.RoundingMode;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigLoadingTest {

    @Test
    public void propertiesCanBeStrippedDownToNativeProperties() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("special.receiver.config", "value");
        configs.put("special.bounded.receiver.config", "value");
        configs.put("atleon.config", "value");
        configs.put("native.config", "value");

        Map<String, Object> result = ConfigLoading.loadNative(configs);

        assertEquals(Collections.singletonMap("native.config", "value"), result);
    }

    @Test
    public void parseableConfigsAreLoadedCorrectly() {
        assertEquals(
            Duration.ofSeconds(10),
            ConfigLoading.loadDurationOrThrow(Collections.singletonMap("duration", "PT10S"), "duration")
        );
        assertTrue(ConfigLoading.loadBooleanOrThrow(Collections.singletonMap("boolean", "true"), "boolean"));
        assertEquals(10, ConfigLoading.loadIntOrThrow(Collections.singletonMap("integer", "10"), "integer"));
        assertEquals(15L, ConfigLoading.loadLongOrThrow(Collections.singletonMap("long", "15"), "long"));
        assertEquals("x", ConfigLoading.loadStringOrThrow(Collections.singletonMap("string", "x"), "string"));
        assertEquals(
            RoundingMode.HALF_UP,
            ConfigLoading.loadEnumOrThrow(Collections.singletonMap("enum", "HALF_UP"), "enum", RoundingMode.class)
        );
        assertEquals(
            TestConfigurable.class,
            ConfigLoading.loadClassOrThrow(Collections.singletonMap("type", TestConfigurable.class.getName()), "type")
        );
    }

    @Test
    public void configsArePassedThroughIfAlreadyTheCorrectType() {
        Duration duration = Duration.ofSeconds(10);

        Map<String, ?> configs = Collections.singletonMap("duration", duration);

        Duration loadedDuration = ConfigLoading.loadDurationOrThrow(configs, "duration");

        assertSame(duration, loadedDuration);
    }

    @Test
    public void configsCanBeLoadedAsSetsOfStrings() {
        assertEquals(
            Stream.of("one").collect(Collectors.toCollection(LinkedHashSet::new)),
            ConfigLoading.loadSetOfStringOrEmpty(Collections.singletonMap("list", "one"), "list")
        );
        assertEquals(
            Stream.of("one", "two", "three").collect(Collectors.toCollection(LinkedHashSet::new)),
            ConfigLoading.loadSetOfStringOrEmpty(Collections.singletonMap("list", "one,two, three"), "list")
        );
    }

    @Test
    public void instancesArePassedThroughIfAlreadyTheCorrectType() {
        Object instance = new TestConfigurable();

        Map<String, ?> configs = Collections.singletonMap("instances", instance);

        List<Configurable> loadedInstances =
            ConfigLoading.loadListOfInstancesOrEmpty(configs, "instances", Configurable.class);

        assertEquals(1, loadedInstances.size());
        assertSame(instance, loadedInstances.get(0));
    }

    @Test
    public void instancesAreCreatedIfClassIsSpecified() {
        Map<String, ?> configs = Collections.singletonMap("instances", TestConfigurable.class);

        List<Configurable> loadedInstances =
            ConfigLoading.loadListOfInstancesOrEmpty(configs, "instances", Configurable.class);

        assertEquals(1, loadedInstances.size());
        assertTrue(loadedInstances.get(0) instanceof TestConfigurable);
    }

    @Test
    public void instancesAreCreatedIfClassNameIsSpecified() {
        Map<String, ?> configs = Collections.singletonMap("instances", TestConfigurable.class.getName());

        List<Configurable> loadedInstances =
            ConfigLoading.loadListOfInstancesOrEmpty(configs, "instances", Configurable.class);

        assertEquals(1, loadedInstances.size());
        assertTrue(loadedInstances.get(0) instanceof TestConfigurable);
    }

    @Test
    public void noInstancesAreCreatedIfClassNamesAreEmpty() {
        Map<String, ?> nullConfigs = Collections.singletonMap("instances", null);
        Map<String, ?> emptyConfigs = Collections.singletonMap("instances", "");

        assertTrue(ConfigLoading.loadListOfInstances(nullConfigs, "instances", Object.class).isPresent());
        assertTrue(ConfigLoading.loadListOfInstances(emptyConfigs, "instances", Object.class).isPresent());
        assertTrue(ConfigLoading.loadListOfInstancesOrEmpty(nullConfigs, "instances", Object.class).isEmpty());
        assertTrue(ConfigLoading.loadListOfInstancesOrEmpty(emptyConfigs, "instances", Object.class).isEmpty());
    }

    @Test
    public void configurablesCanBeLoadedAndConfigured() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("configurable", TestConfigurable.class);
        configs.put(TestConfigurable.VALUE_CONFIG, "configured");

        TestConfigurable configurable = ConfigLoading.loadConfiguredOrThrow(configs, "configurable", TestConfigurable.class);

        assertEquals("configured", configurable.getValue());
    }

    @Test
    public void configurablesWithPredefinedTypesCanBeLoadedAndConfigured() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("configurable", "test");
        configs.put(TestConfigurable.VALUE_CONFIG, "configured");

        Optional<List<TestConfigurable>> result = ConfigLoading.loadListOfConfiguredWithPredefinedTypes(
            configs,
            "configurable",
            TestConfigurable.class,
            typeName -> typeName.equalsIgnoreCase("test")
                ? Optional.of(Collections.singletonList(new TestConfigurable()))
                : Optional.empty()
        );

        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());
        assertEquals("configured", result.get().get(0).getValue());
    }

    public static final class TestConfigurable implements Configurable {

        public static final String VALUE_CONFIG = "value";

        private String value = "unconfigured";

        @Override
        public void configure(Map<String, ?> properties) {
            this.value = ConfigLoading.loadStringOrThrow(properties, VALUE_CONFIG);
        }

        public String getValue() {
            return value;
        }
    }
}