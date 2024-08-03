package io.atleon.core;

import io.atleon.util.ConfigLoading;
import reactor.core.publisher.Mono;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * A {@link ConfigProcessor} that can be configured to load/add key-values from properties files
 */
public class PropertiesFileConfigProcessor implements ConfigProcessor {

    /**
     * Property used to configure a list of properties files to load from
     */
    public static final String FILES_PROPERTY = "atleon.config.properties.files";

    @Override
    public Mono<Map<String, Object>> process(Map<String, Object> configs) {
        if (configs.containsKey(FILES_PROPERTY)) {
            Map<String, Object> result = new HashMap<>(configs);
            result.remove(FILES_PROPERTY);
            ConfigLoading.loadSetOfStringOrEmpty(configs, FILES_PROPERTY)
                .forEach(fileSpec -> processFile(fileSpec, result::put));
            return Mono.just(result);
        } else {
            return Mono.just(configs);
        }
    }

    private void processFile(String fileSpec, BiConsumer<String, Object> propertyConsumer) {
        String[] splitSpec = fileSpec.split(":");
        String fileName = splitSpec[0];
        String prefix = splitSpec.length > 1 ? (splitSpec[1] + ".") : "";
        loadProperties(fileName).forEach((key, value) -> {
            String keyAsString = key.toString();
            if (keyAsString.startsWith(prefix)) {
                propertyConsumer.accept(keyAsString.substring(prefix.length()), value);
            }
        });
    }

    private Properties loadProperties(String fileName) {
        try (InputStream inputStream = new FileInputStream(fileName)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load properties file: " + fileName, e);
        }
    }
}
