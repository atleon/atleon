package io.atleon.aws.sqs;

import io.atleon.core.ConfigSource;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Reactive source of {@link SqsConfig}s used by AloSqs resources.
 */
public class SqsConfigSource extends ConfigSource<SqsConfig, SqsConfigSource> {

    protected SqsConfigSource() {}

    protected SqsConfigSource(String name) {
        super(name);
    }

    protected SqsConfigSource(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        super(propertiesToName);
    }

    public static SqsConfigSource unnamed() {
        return new SqsConfigSource();
    }

    public static SqsConfigSource named(String name) {
        return new SqsConfigSource(name);
    }

    @Override
    protected SqsConfigSource initializeCopy() {
        return new SqsConfigSource();
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {}

    @Override
    protected SqsConfig postProcessProperties(Map<String, Object> properties) {
        return SqsConfig.create(properties);
    }
}
