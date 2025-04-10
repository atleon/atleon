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
 * {@code concurrency} of that stream, the property {@code stream.magnificent.processing.concurrency}
 * would be resolvable and accessible via {@link #concurrency()}. If an explicit stream property is
 * not found, an intermediate fallback is used under the namespace {@code stream.defaults}, so for
 * example, you can set the default {@code concurrency} used by all streams by configuring
 * {@code stream.defaults.concurrency}.
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
     *
     * Looks up the value associated with the provided key from {@link Environment}
     */
    @Override
    public final Optional<String> getProperty(String key) {
        return Optional.ofNullable(environment.getProperty(key));
    }
}
