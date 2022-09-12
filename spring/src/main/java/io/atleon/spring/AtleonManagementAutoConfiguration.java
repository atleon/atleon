package io.atleon.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.List;

@Configuration(proxyBeanMethods = false)
public class AtleonManagementAutoConfiguration {

    @Bean
    public AloStreamStatusService aloStreamStatusService(List<ConfiguredAloStream> listeners) {
        return new ConfiguredAloStreamStatusService(listeners);
    }

    @Configuration(proxyBeanMethods = false)
    @Conditional(RestManagementEnabled.class)
    public static class RestManagementConfiguration {

        @Bean
        public AtleonManagementController managementController(AloStreamStatusService service) {
            return new AtleonManagementController(service);
        }
    }

    public static class RestManagementEnabled implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return Contexts.isClassPresent(context, "org.springframework.web.bind.annotation.RestController")
                && Contexts.isPropertySetToTrue(context, "atleon.management.rest.enabled");
        }
    }
}
