package io.atleon.examples.spring.awssnssqs.config;

import io.atleon.aws.util.AwsConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;

import java.net.URI;
import java.util.Map;

@Configuration
public class SnsConfiguration {

    @Bean
    public SnsAsyncClient snsClient(
        @Qualifier("localSnsEndpoint") URI endpointOverride,
        @Value("#{localAws}") Map<String, ?> awsProperties
    ) {
        return SnsAsyncClient.builder()
            .endpointOverride(endpointOverride)
            .region(AwsConfig.loadRegion(awsProperties).orElse(null))
            .credentialsProvider(AwsConfig.loadCredentialsProvider(awsProperties))
            .build();
    }

    @Bean("snsTopicArn")
    public String snsTopicArn(SnsAsyncClient client, @Value("${example.sns.topic.name}") String topicName) {
        CreateTopicRequest request = CreateTopicRequest.builder().name(topicName).build();
        return client.createTopic(request).join().topicArn();
    }
}
