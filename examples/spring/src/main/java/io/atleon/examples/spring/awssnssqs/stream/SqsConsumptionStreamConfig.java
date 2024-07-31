package io.atleon.examples.spring.awssnssqs.stream;

import io.atleon.aws.sqs.AloSqsReceiver;
import io.atleon.aws.sqs.LongBodyDeserializer;
import io.atleon.aws.sqs.SqsConfigSource;
import io.atleon.core.AloStreamConfig;
import io.atleon.core.ConfigContext;
import io.atleon.examples.spring.awssnssqs.service.NumbersService;
import io.atleon.spring.AutoConfigureStream;

@AutoConfigureStream(SqsConsumptionStream.class)
public class SqsConsumptionStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public SqsConsumptionStreamConfig(ConfigContext context) {
        this.context = context;
    }

    public AloSqsReceiver<Long> buildReceiver() {
        SqsConfigSource configSource = SqsConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.aws.sns.sqs"))
            .with(AloSqsReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class);
        return AloSqsReceiver.create(configSource);
    }

    public String getQueueUrl() {
        return context.getBean("sqsOutputQueueUrl", String.class);
    }

    public NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
