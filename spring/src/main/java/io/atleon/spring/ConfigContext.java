package io.atleon.spring;

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
public final class ConfigContext {

    private final ApplicationContext applicationContext;

    public ConfigContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * Return the bean instance that uniquely matches the given object type, if any.
     */
    public <T> T getBean(Class<T> clazz) throws BeansException {
        return applicationContext.getBean(clazz);
    }

    /**
     * Return the bean instance that uniquely matches the given name and object type, if any.
     */
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
     * Return the property value associated with the given key, or {@link Optional#empty()} if not
     * available.
     */
    public Optional<String> findProperty(String key) {
        return Optional.ofNullable(applicationContext.getEnvironment().getProperty(key));
    }

    /**
     * Return the property value associated with the given key, parsed as the provided type, or
     * {@link Optional#empty()} if not available.
     */
    public <T> Optional<T> findProperty(String key, Class<T> clazz) {
        return Optional.ofNullable(applicationContext.getEnvironment().getProperty(key, clazz));
    }

    /**
     * Get all properties that start with the provided prefix, with the prefix removed.
     */
    public Map<String, String> getPropertiesPrefixedBy(String prefix) {
        return Binder.get(applicationContext.getEnvironment())
            .bind(prefix, Bindable.mapOf(String.class, String.class))
            .orElse(Collections.emptyMap());
    }

    /**
     * Return the property value associated with the given key (never {@code null}).
     *
     * @throws IllegalStateException if the key cannot be resolved
     */
    public String getProperty(String key) throws IllegalStateException {
        return applicationContext.getEnvironment().getRequiredProperty(key);
    }

    /**
     * Return the property value associated with the given key, parsed as the provided type (never
     * {@code null}).
     *
     * @throws IllegalStateException if the key cannot be resolved
     */
    public <T> T getProperty(String key, Class<T> clazz) throws IllegalStateException {
        return applicationContext.getEnvironment().getRequiredProperty(key, clazz);
    }
}
