package io.atleon.examples.spring.kafka.application;

import io.atleon.examples.spring.kafka.stream.KafkaGeneration;
import io.atleon.examples.spring.kafka.stream.KafkaGenerationConfig;
import io.atleon.examples.spring.kafka.stream.KafkaProcessing;
import io.atleon.examples.spring.kafka.stream.KafkaProcessingConfig;
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
    public ApplicationListener<ApplicationContextEvent> kafkaGenerationListener(KafkaGenerationConfig config) {
        return new AloStreamApplicationListener<>(new KafkaGeneration(), config);
    }

    @Bean
    public ApplicationListener<ApplicationContextEvent> kafkaProcessingListener(KafkaProcessingConfig config) {
        return new AloStreamApplicationListener<>(new KafkaProcessing(), config);
    }
}
