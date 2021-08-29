package io.atleon.micrometer;

import org.apache.kafka.common.metrics.KafkaMetric;

import java.util.HashMap;
import java.util.Map;

public class AloKafkaMetricsReporter extends AbstractKafkaMetricsReporter {

    /**
     * Some Metric names are redundantly qualified/prefixed with String representations of their
     * tags (See Fetcher::FetchManagerMetrics). We strip these off and rely on using said tags to
     * differentiate Metrics from one-another.
     */
    @Override
    protected String extractMetricName(KafkaMetric metric) {
        return removeUpToLastAndIncluding(metric.metricName().name(), '.');
    }

    @Override
    protected Map<String, String> extractTags(KafkaMetric metric) {
        Map<String, String> tags = new HashMap<>(super.extractTags(metric));
        tags.computeIfPresent(CLIENT_ID_TAG, (key, value) -> sanitizeClientId(value));
        return tags;
    }

    private String sanitizeClientId(String clientId) {
        return clientId.replaceAll("-\\d+$", "");
    }
}
