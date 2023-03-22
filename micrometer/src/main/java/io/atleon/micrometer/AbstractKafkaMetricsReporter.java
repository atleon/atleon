package io.atleon.micrometer;

import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractKafkaMetricsReporter implements MetricsReporter {

    public enum FilterInclusion {BLACKLIST, WHITELIST}

    public static final String CONFIG_PREFIX = "metric.reporter.";

    public static final String FILTER_NAMES_INCLUSION_CONFIG = CONFIG_PREFIX + "filter.names.inclusion";

    public static final String FILTER_NAMES_CONFIG = CONFIG_PREFIX + "filter.names";

    protected static final String CLIENT_ID_TAG = "client-id";

    private static final Logger LOGGER = Logger.getLogger(AbstractKafkaMetricsReporter.class.getName());

    private static final double ABSENT_EVALUATION = Double.NaN;

    private static final AuditedMetricRegistry<AbstractKafkaMetricsReporter, KafkaMetric, Double> AUDITED_METRIC_REGISTRY =
        new AuditedMetricRegistry<>(AbstractKafkaMetricsReporter::extractMeterValue, ABSENT_EVALUATION);

    protected MeterRegistry meterRegistry = Metrics.globalRegistry;

    private FilterInclusion filteredMetricNamesInclusion = FilterInclusion.BLACKLIST;

    private Collection<String> filteredMetricNames = Collections.emptySet();

    @Override
    public void configure(Map<String, ?> configs) {
        this.meterRegistry = createMeterRegistry(configs);
        this.filteredMetricNamesInclusion = loadFilterInclusion(configs).orElse(FilterInclusion.BLACKLIST);
        this.filteredMetricNames = ConfigLoading.loadSetOfStringOrEmpty(configs, FILTER_NAMES_CONFIG);
    }

    @Override
    public final void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public final void metricChange(KafkaMetric metric) {
        String extractedMetricName = extractMetricName(metric);
        if (shouldReportMetric(metric, extractedMetricName)) {
            registerMetric(createMeterKey(metric, extractedMetricName), metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        AUDITED_METRIC_REGISTRY.unregister(createMeterKey(metric, extractMetricName(metric)), this);
    }

    @Override
    public void close() {
        AUDITED_METRIC_REGISTRY.unregister(this);
    }

    protected MeterRegistry createMeterRegistry(Map<String, ?> configs) {
        return Metrics.globalRegistry;
    }

    protected String extractMetricName(KafkaMetric metric) {
        return metric.metricName().name();
    }

    protected boolean shouldReportMetric(KafkaMetric metric, String extractedMetricName) {
        return FilterInclusion.WHITELIST.equals(filteredMetricNamesInclusion) == filteredMetricNames.contains(extractedMetricName);
    }

    protected final MeterKey createMeterKey(KafkaMetric metric, String extractedMetricName) {
        return new MeterKey(extractMeterNamePrefix(metric) + extractedMetricName, extractTags(metric));
    }

    protected String extractMeterNamePrefix(KafkaMetric metric) {
        return "kafka." + metric.metricName().group() + ".";
    }

    protected Map<String, String> extractTags(KafkaMetric metric) {
        return metric.metricName().tags();
    }

    protected final void registerMetric(MeterKey meterKey, KafkaMetric metric) {
        // Note that the MeterKey used to register a Gauge is the one returned by registration
        // with the AuditedMetricRegistry, which keeps an interned Strong Reference to all past
        // and current registered MeterKeys in order to prevent removal of said MeterKeys by
        // garbage collection, which is possible due to Weak Referencing of MeterKeys by underlying
        // MeterRegistry implementations
        MeterKey registeredMeterKey = AUDITED_METRIC_REGISTRY.register(meterKey, this, metric);
        if (meterRegistry.find(registeredMeterKey.getName()).tags(registeredMeterKey.getTags()).meter() == null) {
            registerGauge(meterRegistry, registeredMeterKey, metric);
        }
    }

    protected final double extractMeterValue(KafkaMetric metric) {
        return Optional.ofNullable(metric.metricValue())
            .filter(Number.class::isInstance)
            .map(Number.class::cast)
            .flatMap(this::extractMeterValue)
            .orElse(ABSENT_EVALUATION);
    }

    protected Optional<Double> extractMeterValue(Number number) {
        return Optional.of(number.doubleValue());
    }

    protected static String removeUuids(String string) {
        return string.replaceAll("-?[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", "");
    }

    protected static String removeUpToLastAndIncluding(String string, char toRemove) {
        return string.substring(string.lastIndexOf(toRemove) + 1);
    }

    private static Optional<FilterInclusion> loadFilterInclusion(Map<String, ?> configs) {
        return ConfigLoading.loadParseable(
            configs,
            FILTER_NAMES_INCLUSION_CONFIG,
            FilterInclusion.class,
            FilterInclusion::valueOf
        );
    }

    private static void registerGauge(MeterRegistry meterRegistry, MeterKey meterKey, KafkaMetric metric) {
        try {
            // Note that depending on the MeterRegistry implementation, the registered MeterKey may
            // only be Weakly Referenced, which means it is susceptible garbage collection if not
            // Strongly Referenced elsewhere (i.e. by an AuditedMetricRegistry)
            Gauge.builder(meterKey.getName(), meterKey, AUDITED_METRIC_REGISTRY::evaluate)
                .description(metric.metricName().description())
                .tags(meterKey.getTags())
                .register(meterRegistry);
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Failed to register Gauge with key=" + meterKey, e);
        }
    }
}
