package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;

import java.util.List;

public class BatchRequestFailedException extends RuntimeException {

    BatchRequestFailedException(String type, List<BatchResultErrorEntry> entries) {
        super(String.format("Batch request failed! type=%s errors=%s", type, entries));
    }
}
