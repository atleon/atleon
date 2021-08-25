package io.atleon.core;

import io.atelon.util.ConfigLoading;

import java.util.HashMap;
import java.util.Map;

public final class EnvironmentalConfigs implements ConfigInterceptor {

    public static final String PREFIX = "io.atleon.config.";

    @Override
    public Map<String, Object> intercept(String name, Map<String, Object> configs) {
        String prefix = String.format("%s%s.", PREFIX, name);
        Map<String, Object> result = new HashMap<>(configs);
        result.putAll(ConfigLoading.loadPrefixedEnvironmentalProperties(prefix));
        return result;
    }

    @Override
    public Map<String, Object> intercept(Map<String, Object> configs) {
        return configs;
    }
}
