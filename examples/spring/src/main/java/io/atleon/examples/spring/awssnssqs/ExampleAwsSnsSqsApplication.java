package io.atleon.examples.spring.awssnssqs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.ConfigurableEnvironment;

@SpringBootApplication
public class ExampleAwsSnsSqsApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(ExampleAwsSnsSqsApplication.class) {
            @Override
            protected void configureProfiles(ConfigurableEnvironment environment, String[] args) {
                environment.setActiveProfiles("aws");
            }
        };
        springApplication.run(args);
    }
}
