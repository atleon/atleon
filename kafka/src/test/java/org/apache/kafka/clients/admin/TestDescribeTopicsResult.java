package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;

import java.util.Map;

/**
 * Test extension of {@link DescribeTopicsResult} to allow creation for tests
 */
public class TestDescribeTopicsResult extends DescribeTopicsResult {

    public TestDescribeTopicsResult(Map<String, KafkaFuture<TopicDescription>> futures) {
        super(null, futures);
    }
}
