package io.atleon.spring;

import io.atleon.aws.sns.SnsConfigSource;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.core.ConfigSource;
import io.atleon.kafka.KafkaConfigSource;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.Environment;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Dynamically registers {@link io.atleon.core.ConfigSource} beans under the property
 * {@link #SPECS_PROPERTY}.
 */
public class ConfigSourcesRegistrar implements BeanDefinitionRegistryPostProcessor {

    private static final String SPECS_PROPERTY = "atleon.config.sources";

    private static final String FACTORY_BEAN_NAME = "atleonConfigSourceFactory";

    private static final ResolvableType ENTRIES_TYPE =
        ResolvableType.forClassWithGenerics(List.class,
            ResolvableType.forClassWithGenerics(Map.class, String.class, String.class));

    private final List<Map<String, ?>> specs;

    public ConfigSourcesRegistrar(Environment environment) {
        this.specs = Binder.get(environment)
            .bind(SPECS_PROPERTY, Bindable.<List<Map<String, ?>>>of(ENTRIES_TYPE))
            .orElse(Collections.emptyList());
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        BeanDefinition factoryDefinition =
            BeanDefinitionBuilder.genericBeanDefinition(Factory.class).getBeanDefinition();
        registry.registerBeanDefinition(FACTORY_BEAN_NAME, factoryDefinition);
        specs.forEach(it -> register(it, registry::registerBeanDefinition));
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

    }

    private static void register(Map<String, ?> spec, BiConsumer<String, BeanDefinition> registrar) {
        Map<String, Object> mutableSpec = new HashMap<>(spec);
        String name = Objects.requireNonNull(mutableSpec.remove("name"), "Spec must have name").toString();
        String type = Objects.requireNonNull(mutableSpec.remove("type"), "Spec must have type").toString();
        registrar.accept(name, newDefinition(name, type, mutableSpec));
    }

    private static BeanDefinition newDefinition(String name, String type, Map<String, ?> properties) {
        switch (type) {
            case "kafka":
                return newDefinition(KafkaConfigSource.class, () -> KafkaConfigSource.named(name), properties);
            case "rabbitMQ":
                return newDefinition(RabbitMQConfigSource.class, () -> RabbitMQConfigSource.named(name), properties);
            case "sns":
                return newDefinition(SnsConfigSource.class, () -> SnsConfigSource.named(name), properties);
            case "sqs":
                return newDefinition(SqsConfigSource.class, () -> SqsConfigSource.named(name), properties);
            default:
                throw new IllegalArgumentException("Unsupported config source type: " + type);
        }
    }

    private static <T, S extends ConfigSource<T, S>> BeanDefinition newDefinition(
        Class<S> configType,
        Supplier<S> configCreator,
        Map<String, ?> properties
    ) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(configType);
        builder.setFactoryMethodOnBean("create", FACTORY_BEAN_NAME);
        builder.addConstructorArgValue(configCreator);
        builder.addConstructorArgValue(properties);
        return builder.getBeanDefinition();
    }

    public static final class Factory {

        public <T, S extends ConfigSource<T, S>> S create(Supplier<S> configCreator, Map<String, ?> properties) {
            return configCreator.get().withAll(properties);
        }
    }
}
