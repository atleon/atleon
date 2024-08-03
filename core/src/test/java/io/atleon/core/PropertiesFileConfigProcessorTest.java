package io.atleon.core;

import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertiesFileConfigProcessorTest {

    @Test
    public void propertiesCanBeLoadedFromFiles() {
        Properties properties = new Properties();
        properties.put("foo.key", "fooValue");
        properties.put("bar.key", "barValue");

        Path propertiesFilePath = writePropertiesFile(properties, "noprefix");

        Map<String, Object> result = new DummyConfigSource()
            .with(ConfigSource.PROCESSORS_PROPERTY, PropertiesFileConfigProcessor.class)
            .with(PropertiesFileConfigProcessor.FILES_PROPERTY, propertiesFilePath.toString())
            .create()
            .block();

        properties.forEach((key, value) -> assertEquals(value, result.get(key), key.toString()));
    }

    @Test
    public void propertiesCanBeLoadedFromFilesByPrefix() {
        String prefix = "foo";
        Properties properties = new Properties();
        properties.put("foo.fooKey", "fooValue");
        properties.put("bar.varKey", "barValue");

        Path propertiesFilePath = writePropertiesFile(properties, "prefix");

        Map<String, Object> result = new DummyConfigSource()
            .with(ConfigSource.PROCESSORS_PROPERTY, PropertiesFileConfigProcessor.class)
            .with(PropertiesFileConfigProcessor.FILES_PROPERTY, propertiesFilePath + ":" + prefix)
            .create()
            .block();

        assertEquals(properties.get("foo.fooKey"), result.get("fooKey"));
    }

    private Path writePropertiesFile(Properties properties, String qualifier) {
        String fileName = PropertiesFileConfigProcessorTest.class.getSimpleName() + "-" + qualifier;
        try {
            Path filePath = Files.createTempFile(fileName, ".properties");
            properties.store(new FileOutputStream(filePath.toFile()), qualifier);
            return filePath;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write properties file", e);
        }
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