package io.atleon.opentracing;

import io.atleon.aws.sqs.AloReceivedSqsMessageDecorator;
import io.atleon.aws.sqs.ReceivedSqsMessage;
import io.atleon.core.Alo;
import io.atleon.util.ConfigLoading;
import io.opentracing.Tracer;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * An {@link AloReceivedSqsMessageDecorator} that decorates {@link Alo} elements with tracing
 * context extracted from {@link ReceivedSqsMessage}s
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedSqsMessage}s
 */
public class TracingAloReceivedSqsMessageDecorator<T>
    extends TracingAloConsumptionDecorator<ReceivedSqsMessage<T>>
    implements AloReceivedSqsMessageDecorator<T> {

    private String queueUrl = null;

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queueUrl = ConfigLoading.loadString(properties, QUEUE_URL_CONFIG).orElse(queueUrl);
    }

    @Override
    protected Tracer.SpanBuilder newSpanBuilder(SpanBuilderFactory spanBuilderFactory, ReceivedSqsMessage<T> message) {
        return spanBuilderFactory.newSpanBuilder("atleon.aws.sqs.consume")
            .withTag("queueUrl", queueUrl)
            .withTag("receiptHandle", message.receiptHandle())
            .withTag("messageId", message.messageId());
    }

    @Override
    protected Map<String, String> extractHeaderMap(ReceivedSqsMessage<T> message) {
        return message.messageAttributes().entrySet().stream()
            .filter(entry -> entry.getValue().stringValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().stringValue()));
    }
}
