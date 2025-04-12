package io.atleon.spring;

import io.atleon.core.PropertiedAloStreamConfig;
import io.atleon.core.SelfConfigurableAloStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import java.util.Optional;

/**
 * Spring-aware extension of {@link SelfConfigurableAloStream} that provides convenient access to
 * Spring properties. By implementing {@link PropertiedAloStreamConfig}, stream-specific properties
 * are located under a dot-notated prefix derived from the name of this stream. For example, given
 * default method usage, a stream named {@code MagnificentProcessingStream} will have its
 * properties available under the prefix {@code stream.magnificent.processing}, and to configure
 * a property like {@code concurrency} of that stream (for example), the property
 * {@code stream.magnificent.processing.concurrency} would be resolvable and accessible via
 * {@code getStreamProperty("concurrency", Integer::parse, 1)}. If an explicit stream property is
 * not found, an intermediate default is configurable under the namespace {@code stream.defaults},
 * so as an example, you can set the default value for {@code concurrency} used by all extending
 * streams by configuring {@code stream.defaults.concurrency}.
 */
public abstract class SpringAloStream extends SelfConfigurableAloStream implements PropertiedAloStreamConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringAloStream.class);

    private final Environment environment;

    protected SpringAloStream(ApplicationContext context) {
        this.environment = context.getEnvironment();
        LOGGER.info("Stream properties for {} configurable under '{}'", name(), streamPropertyNamespace());
    }

    /**
     * {@inheritDoc}
     * <p>
     * By default, this method takes the name of the stream (which is conventionally kebab-cased),
     * removes any existing "-stream" suffix, converts it to dot-notation, and appends the result
     * to "stream.". So, for example, a stream named {@code MagnificentProcessingStream} would have
     * a property namespace of {@code stream.magnificent.processing}.
     */
    @Override
    public String streamPropertyNamespace() {
        return "stream." + name().replaceAll("-stream(-\\w?\\d+)?$", "").replace('-', '.');
    }

    /**
     * {@inheritDoc}
     * <p>
     * By default, locates default properties under the namespace {@code stream.defaults}
     */
    @Override
    public String defaultablePropertyNamespace() {
        return "stream.defaults";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Looks up the value associated with the provided key from {@link Environment}
     */
    @Override
    public <T> Optional<T> getProperty(String key, Class<T> type) {
        return Optional.ofNullable(environment.getProperty(key, type));
    }
}
