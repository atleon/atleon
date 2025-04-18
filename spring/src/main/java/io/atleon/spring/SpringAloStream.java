package io.atleon.spring;

import io.atleon.core.Autostart;
import io.atleon.core.SelfConfigurableAloStream;
import io.atleon.core.StreamPropertyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import java.util.Optional;

/**
 * Spring-aware extension of {@link SelfConfigurableAloStream} that provides convenient access to
 * Spring properties. By implementing {@link StreamPropertyResolver}, stream-specific properties
 * are located under a dot-notated prefix derived from the name of this stream. For example, given
 * default method usage, a stream named {@code MagnificentProcessingStream} will have its
 * properties available under the prefix {@code stream.magnificent.processing}, and to configure
 * a property like {@code concurrency} of that stream (for example), the property
 * {@code stream.magnificent.processing.concurrency} would be resolvable and accessible via
 * {@code getStreamProperty("concurrency", Integer.class, 1)}. If an explicit stream property is
 * not found, an intermediate default is configurable under the namespace {@code stream.defaults},
 * so as an example, you can set the default value for {@code concurrency} used by all extending
 * streams by configuring {@code stream.defaults.concurrency}.
 */
public abstract class SpringAloStream extends SelfConfigurableAloStream implements StreamPropertyResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringAloStream.class);

    private final Environment environment;

    protected SpringAloStream(ApplicationContext context) {
        this.environment = context.getEnvironment();
        LOGGER.info("Stream properties for {} configurable under '{}'", name(), specificStreamPropertyNamespace());
    }

    /**
     * {@inheritDoc}
     * <p>
     * By default, makes autostart configurable under {@code autostart} stream property (as
     * {@code "ENABLED"} or {@code "DISABLED"}).
     */
    @Override
    public Autostart autostart() {
        return getStreamProperty("autostart", Autostart.class).orElse(super.autostart());
    }

    /**
     * {@inheritDoc}
     * <p>
     * By default, this method takes the name of the stream (which is conventionally kebab-cased),
     * removes any existing "-stream" suffix, converts it to dot-notation, and appends the result
     * to "stream.". So, for example, a stream named {@code MagnificentProcessingStream} would have
     * an explicit property namespace of {@code stream.magnificent.processing}.
     */
    @Override
    public String specificStreamPropertyNamespace() {
        return "stream." + name().replaceAll("-stream(-\\w?\\d+)?$", "").replace('-', '.');
    }

    /**
     * {@inheritDoc}
     * <p>
     * By default, locates default stream properties under the namespace {@code stream.defaults}
     */
    @Override
    public String defaultStreamPropertyNamespace() {
        return "stream.defaults";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Looks up the value associated with the provided key from {@link Environment}
     */
    @Override
    public final <T> Optional<T> getProperty(String key, Class<T> type) {
        return Optional.ofNullable(environment.getProperty(key, type));
    }
}
