package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.examples.spring.awssnssqs.stream.SnsGeneration;
import io.atleon.examples.spring.awssnssqs.stream.SnsGenerationConfig;
import io.atleon.examples.spring.awssnssqs.stream.SqsProcessing;
import io.atleon.examples.spring.awssnssqs.stream.SqsProcessingConfig;
import io.atleon.spring.AloStreamApplicationListener;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * Configuration that binds Stream with their Configs in to ApplicationListeners that will start
 * and stop with Application lifecycle
 */
@Configuration
public class StreamConfiguration {

    @Bean
    public ApplicationListener<ApplicationContextEvent> rabbitMQGenerationListener(SnsGenerationConfig config) {
        return new AloStreamApplicationListener<>(new SnsGeneration(), config);
    }

    @Bean
    public ApplicationListener<ApplicationContextEvent> rabbitMQProcessingListener(SqsProcessingConfig config) {
        return new AloStreamApplicationListener<>(new SqsProcessing(), config);
    }
}
