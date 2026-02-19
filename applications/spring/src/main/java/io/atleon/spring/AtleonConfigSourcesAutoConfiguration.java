package io.atleon.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Configuration(proxyBeanMethods = false)
public class AtleonConfigSourcesAutoConfiguration {

    @Bean
    @Conditional(ConfigSourcesRegistrationEnabled.class)
    public ConfigSourcesRegistrar atleonConfigSourcesRegistrar(Environment environment) {
        return new ConfigSourcesRegistrar(environment);
    }

    public static final class ConfigSourcesRegistrationEnabled implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return !Contexts.isPropertySetToFalse(context, "atleon.registration.config.sources.enabled");
        }
    }
}
