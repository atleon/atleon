package io.atleon.examples.spring.kafka.application;

import io.atleon.kafka.KafkaConfigSource;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfiguration {

    private final String bootstrapServers = EmbeddedKafka.startAndGetBootstrapServersConnect();

    @Bean("local")
    public KafkaConfigSource localKafka() {
        return KafkaConfigSource.useClientIdAsName()
            .withBootstrapServers(bootstrapServers);
    }
}
