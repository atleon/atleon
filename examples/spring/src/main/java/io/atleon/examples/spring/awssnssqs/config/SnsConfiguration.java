package io.atleon.examples.spring.awssnssqs.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;

import java.net.URI;

@Configuration
public class SnsConfiguration {

    @Bean
    public SnsAsyncClient snsClient(
        @Qualifier("snsEndpointOverride") URI endpointOverride,
        @Qualifier("awsRegion") String region,
        @Qualifier("awsAccessKeyId") String accessKeyId,
        @Qualifier("awsSecretAccessKey") String secretAccessKey
    ) {
        return SnsAsyncClient.builder()
            .endpointOverride(endpointOverride)
            .region(Region.of(region))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
            .build();
    }

    @Bean("snsTopicArn")
    public String snsTopicArn(SnsAsyncClient client, @Value("${example.sns.topic.name}") String topicName) {
        CreateTopicRequest request = CreateTopicRequest.builder().name(topicName).build();
        return client.createTopic(request).join().topicArn();
    }
}
