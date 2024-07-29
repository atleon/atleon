package io.atleon.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AtleonConfigContextAutoConfiguration {

    @Bean
    public ConfigContext configContext(ApplicationContext applicationContext) {
        return new ConfigContext(applicationContext);
    }
}
