package io.atleon.micrometer;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class AuditedMetricRegistry<A, M, R> {

    private final Map<MeterKey, MeterKey> internedKeys = new ConcurrentHashMap<>();

    private final Map<MeterKey, AuditedMetric<A, M>> auditedMetricsByKey = new ConcurrentHashMap<>();

    private final BiFunction<A, M, R> evaluator;

    private final R absentEvaluation;

    public AuditedMetricRegistry(BiFunction<A, M, R> evaluator, R absentEvaluation) {
        this.evaluator = evaluator;
        this.absentEvaluation = absentEvaluation;
    }

    public MeterKey register(MeterKey key, A auditor, M metric) {
        MeterKey internedKey = internKey(key);
        AuditedMetric<A, M> auditedMetric = new AuditedMetric<>(auditor, metric);
        if (!Objects.equals(auditedMetricsByKey.get(internedKey), auditedMetric)) {
            synchronized (auditedMetricsByKey) {
                auditedMetricsByKey.put(internedKey, auditedMetric);
            }
        }
        return internedKey;
    }

    public void unregister(MeterKey key, A auditor) {
        synchronized (auditedMetricsByKey) {
            if (isCurrentAuditor(key, auditor)) {
                auditedMetricsByKey.remove(key);
            }
        }
    }

    public void unregister(A auditor) {
        synchronized (auditedMetricsByKey) {
            auditedMetricsByKey.values().removeIf(auditedMetric -> auditedMetric.isAuditedBy(auditor));
        }
    }

    public R evaluate(MeterKey key) {
        return Optional.ofNullable(auditedMetricsByKey.get(key))
                .map(auditedMetric -> auditedMetric.evaluate(evaluator))
                .orElse(absentEvaluation);
    }

    private MeterKey internKey(MeterKey key) {
        MeterKey internedKey = internedKeys.get(key);
        if (internedKey == null) {
            synchronized (internedKeys) {
                internedKey = internedKeys.computeIfAbsent(key, Function.identity());
            }
        }
        return internedKey;
    }

    private boolean isCurrentAuditor(MeterKey key, A auditor) {
        return Optional.ofNullable(auditedMetricsByKey.get(key))
                .map(auditedMetric -> auditedMetric.isAuditedBy(auditor))
                .orElse(false);
    }

    private static final class AuditedMetric<A, M> {

        private final A auditor;

        private final M metric;

        public AuditedMetric(A auditor, M metric) {
            this.auditor = auditor;
            this.metric = metric;
        }

        public <R> R evaluate(BiFunction<A, M, R> evaluator) {
            return evaluator.apply(auditor, metric);
        }

        public boolean isAuditedBy(A otherAuditor) {
            return Objects.equals(auditor, otherAuditor);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AuditedMetric<?, ?> that = (AuditedMetric<?, ?>) o;
            return Objects.equals(auditor, that.auditor) && Objects.equals(metric, that.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(auditor, metric);
        }
    }
}
