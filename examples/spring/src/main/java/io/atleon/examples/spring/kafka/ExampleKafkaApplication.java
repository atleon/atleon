package io.atleon.examples.spring.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.ConfigurableEnvironment;

@SpringBootApplication
public class ExampleKafkaApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(ExampleKafkaApplication.class) {
            @Override
            protected void configureProfiles(ConfigurableEnvironment environment, String[] args) {
                environment.setActiveProfiles("kafka");
            }
        };
        springApplication.run(args);
    }
}
