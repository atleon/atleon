package io.atleon.examples.spring.awssnssqs.config;

import java.util.function.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("!integrationTest")
@Configuration
public class NumbersConfiguration {

    @Bean("specialNumberConsumer")
    public Consumer<Number> specialNumberConsumer() {
        return number -> System.out.println("Found a special number: " + number);
    }
}
