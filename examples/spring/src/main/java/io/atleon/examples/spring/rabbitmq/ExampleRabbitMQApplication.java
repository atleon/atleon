package io.atleon.examples.spring.rabbitmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.ConfigurableEnvironment;

@SpringBootApplication
public class ExampleRabbitMQApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(ExampleRabbitMQApplication.class) {
            @Override
            protected void configureProfiles(ConfigurableEnvironment environment, String[] args) {
                environment.setActiveProfiles("rabbitmq");
            }
        };
        springApplication.run(args);
    }
}
