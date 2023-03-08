package io.atleon.opentracing;

import io.atleon.aws.sqs.AloReceivedSqsMessageDecorator;
import io.atleon.aws.sqs.ReceivedSqsMessage;
import io.atleon.core.Alo;
import io.opentracing.References;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TracingAloReceivedSqsMessageDecorator<T> extends ConsumerTracing implements AloReceivedSqsMessageDecorator<T> {
    @Override
    public Alo<ReceivedSqsMessage<T>> decorate(Alo<ReceivedSqsMessage<T>> alo) {
        ReceivedSqsMessage<T> receivedSqsMessage = alo.get();
        Tracer.SpanBuilder spanBuilder = newSpanBuilder("atleon.aws.sqs.consume")
            .withTag("receiptHandle", receivedSqsMessage.receiptHandle())
            .withTag("messageId", receivedSqsMessage.messageId());
        extractSpanContext(receivedSqsMessage)
            .ifPresent(it -> spanBuilder.addReference(References.FOLLOWS_FROM, it));
        return TracingAlo.start(alo, tracerFacade(), spanBuilder);
    }

    protected Optional<SpanContext> extractSpanContext(ReceivedSqsMessage<?> receivedSqsMessage) {
        Map<String, String> headerMap = receivedSqsMessage.messageAttributes().entrySet().stream()
            .filter(entry -> entry.getValue().stringValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().stringValue()));
        return extractSpanContext(headerMap);
    }
}
