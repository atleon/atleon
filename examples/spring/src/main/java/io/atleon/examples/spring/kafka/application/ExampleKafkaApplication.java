package io.atleon.examples.spring.kafka.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "io.atleon.examples.spring.kafka")
public class ExampleKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExampleKafkaApplication.class, args);
    }
}
