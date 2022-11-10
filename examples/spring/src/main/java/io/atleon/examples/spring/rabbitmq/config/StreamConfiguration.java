package io.atleon.examples.spring.rabbitmq.config;

import io.atleon.examples.spring.rabbitmq.stream.RabbitMQGeneration;
import io.atleon.examples.spring.rabbitmq.stream.RabbitMQGenerationConfig;
import io.atleon.examples.spring.rabbitmq.stream.RabbitMQProcessing;
import io.atleon.examples.spring.rabbitmq.stream.RabbitMQProcessingConfig;
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
    public ApplicationListener<ApplicationContextEvent> rabbitMQGenerationListener(RabbitMQGenerationConfig config) {
        return new AloStreamApplicationListener<>(new RabbitMQGeneration(), config);
    }

    @Bean
    public ApplicationListener<ApplicationContextEvent> rabbitMQProcessingListener(RabbitMQProcessingConfig config) {
        return new AloStreamApplicationListener<>(new RabbitMQProcessing(), config);
    }
}
