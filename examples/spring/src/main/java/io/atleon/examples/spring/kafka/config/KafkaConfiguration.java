package io.atleon.examples.spring.kafka.config;

import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    private final String bootstrapServers = EmbeddedKafka.startAndGetBootstrapServersConnect();

    @Bean("localKafka")
    public Map<String, Object> localKafka() {
        return Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }
}
