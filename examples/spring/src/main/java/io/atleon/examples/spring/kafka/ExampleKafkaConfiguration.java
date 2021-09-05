package io.atleon.examples.spring.kafka;

import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import io.atleon.spring.AloStreamApplicationListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ApplicationContextEvent;

@Configuration
public class ExampleKafkaConfiguration {

    private final String bootstrapServers = EmbeddedKafka.startAndGetBootstrapServersConnect();

    @Bean("local")
    public KafkaConfigSource localKafka() {
        return KafkaConfigSource.useClientIdAsName()
            .withBootstrapServers(bootstrapServers);
    }

    @Bean
    public ExampleKafkaGenerationConfig kafkaGenerationConfig(
        @Qualifier("local") KafkaConfigSource localKafkaConfig,
        @Value("${example.kafka.topic}") String topic) {
        return new ExampleKafkaGenerationConfig(localKafkaConfig, topic);
    }

    @Bean
    public ExampleKafkaProcessingConfig kafkaProcessingConfig(
        @Qualifier("local") KafkaConfigSource localKafkaConfig,
        @Value("${example.kafka.topic}") String topic) {
        return new ExampleKafkaProcessingConfig(localKafkaConfig, topic, System.out::println);
    }

    @Bean
    public ApplicationListener<ApplicationContextEvent> kafkaGenerationListener(ExampleKafkaGenerationConfig config) {
        return new AloStreamApplicationListener<>(new ExampleKafkaGeneration(), config);
    }

    @Bean
    public ApplicationListener<ApplicationContextEvent> kafkaProcessingListener(ExampleKafkaProcessingConfig config) {
        return new AloStreamApplicationListener<>(new ExampleKafkaProcessing(), config);
    }
}
