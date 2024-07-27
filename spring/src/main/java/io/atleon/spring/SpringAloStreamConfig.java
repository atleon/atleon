package io.atleon.spring;

import io.atleon.core.AloStreamConfig;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.ResolvableType;

import java.util.Optional;

/**
 * Convenient base implementation of {@link AloStreamConfig} for use in Spring applications. When
 * used, extensions have automatic programmatic access to beans and properties accessible via
 * {@link ApplicationContext}. This should remove the necessity for instance variables, along with
 * the boilerplate initialization code commonly required by such variables.
 */
public abstract class SpringAloStreamConfig implements AloStreamConfig {

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Return the bean instance that uniquely matches the given object type, if any.
     */
    protected <T> T getBean(Class<T> clazz) throws BeansException {
        return applicationContext.getBean(clazz);
    }

    /**
     * Return the bean instance that uniquely matches the given name and object type, if any.
     */
    protected <T> T getBean(String name, Class<T> clazz) throws BeansException {
        return applicationContext.getBean(name, clazz);
    }

    /**
     * Return the bean instance that uniquely matches the given {@link ResolvableType}, if any.
     */
    protected <T> T getBean(ResolvableType type) throws BeansException {
        return applicationContext.<T>getBeanProvider(type).getObject();
    }

    /**
     * Return the property value associated with the given key, or {@link Optional#empty()} if not
     * available.
     */
    protected Optional<String> findProperty(String key) {
        return Optional.ofNullable(applicationContext.getEnvironment().getProperty(key));
    }

    /**
     * Return the property value associated with the given key, parsed as the provided type, or
     * {@link Optional#empty()} if not available.
     */
    protected <T> Optional<T> findProperty(String key, Class<T> clazz) {
        return Optional.ofNullable(applicationContext.getEnvironment().getProperty(key, clazz));
    }

    /**
     * Return the property value associated with the given key (never {@code null}).
     * @throws IllegalStateException if the key cannot be resolved
     */
    protected String getProperty(String key) throws IllegalStateException {
        return applicationContext.getEnvironment().getRequiredProperty(key);
    }

    /**
     * Return the property value associated with the given key, parsed as the provided type (never
     * {@code null}).
     * @throws IllegalStateException if the key cannot be resolved
     */
    protected <T> T getProperty(String key, Class<T> clazz) throws IllegalStateException {
        return applicationContext.getEnvironment().getRequiredProperty(key, clazz);
    }
}
