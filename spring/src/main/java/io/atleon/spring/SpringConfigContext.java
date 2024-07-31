package io.atleon.spring;

import io.atleon.core.ConfigContext;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContext;
import org.springframework.core.ResolvableType;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * A facade around {@link ApplicationContext} used to access beans and properties conventionally
 * used by Atleon Config implementations.
 */
public final class SpringConfigContext implements ConfigContext {

    private final ApplicationContext applicationContext;

    private SpringConfigContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public static SpringConfigContext create(ApplicationContext applicationContext) {
        return new SpringConfigContext(applicationContext);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getBean(Class<T> clazz) throws BeansException {
        return applicationContext.getBean(clazz);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getBean(String name, Class<T> clazz) throws BeansException {
        return applicationContext.getBean(name, clazz);
    }

    /**
     * Return the bean instance that uniquely matches the given {@link ResolvableType}, if any.
     */
    public <T> T getBean(ResolvableType type) throws BeansException {
        return applicationContext.<T>getBeanProvider(type).getObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getPropertiesPrefixedBy(String prefix) {
        return Binder.get(applicationContext.getEnvironment())
            .bind(prefix, Bindable.mapOf(String.class, String.class))
            .orElse(Collections.emptyMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getProperty(String key, Class<T> clazz, T defaultValue) {
        return applicationContext.getEnvironment().getProperty(key, clazz, defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProperty(String key) throws IllegalStateException {
        return applicationContext.getEnvironment().getRequiredProperty(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getProperty(String key, Class<T> clazz) throws IllegalStateException {
        return applicationContext.getEnvironment().getRequiredProperty(key, clazz);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> findProperty(String key) {
        return Optional.ofNullable(applicationContext.getEnvironment().getProperty(key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Optional<T> findProperty(String key, Class<T> clazz) {
        return Optional.ofNullable(applicationContext.getEnvironment().getProperty(key, clazz));
    }
}
