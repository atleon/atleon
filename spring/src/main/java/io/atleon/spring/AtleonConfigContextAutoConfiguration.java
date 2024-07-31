package io.atleon.spring;

import io.atleon.core.ConfigContext;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Configuration(proxyBeanMethods = false)
public class AtleonConfigContextAutoConfiguration {

    @Bean
    @Conditional(NoConfigContextBeanRegistered.class)
    public SpringConfigContext springConfigContext(ApplicationContext applicationContext) {
        return SpringConfigContext.create(applicationContext);
    }

    public static final class NoConfigContextBeanRegistered implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            ListableBeanFactory beanFactory = context.getBeanFactory();
            return beanFactory != null && beanFactory.getBeanNamesForType(ConfigContext.class, true, false).length == 0;
        }
    }
}
