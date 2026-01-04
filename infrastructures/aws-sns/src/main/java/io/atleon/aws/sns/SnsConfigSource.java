package io.atleon.aws.sns;

import io.atleon.core.ConfigSource;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Reactive source of {@link SnsConfig}s used by AloSqs resources.
 */
public class SnsConfigSource extends ConfigSource<SnsConfig, SnsConfigSource> {

    protected SnsConfigSource() {}

    protected SnsConfigSource(String name) {
        super(name);
    }

    protected SnsConfigSource(Function<Map<String, Object>, Optional<String>> propertiesToName) {
        super(propertiesToName);
    }

    public static SnsConfigSource unnamed() {
        return new SnsConfigSource();
    }

    public static SnsConfigSource named(String name) {
        return new SnsConfigSource(name);
    }

    @Override
    protected SnsConfigSource initializeCopy() {
        return new SnsConfigSource();
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {}

    @Override
    protected SnsConfig postProcessProperties(Map<String, Object> properties) {
        return SnsConfig.create(properties);
    }
}
