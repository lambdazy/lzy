package ai.lzy.service;

import ai.lzy.metrics.MetricReporter;
import io.prometheus.client.Gauge;
import jakarta.inject.Named;
import jakarta.inject.Singleton;


@Singleton
public class LzyServiceMetrics {
    private static final String LZY_SERVICE = "lzy_service";

    public LzyServiceMetrics(@Named("LzyServiceMetricReporter") MetricReporter ignored) {
    }

    public final Gauge activeExecutions = Gauge
        .build("active_executions", "Active Executions Count")
        .subsystem(LZY_SERVICE)
        .labelNames("user")
        .register();
}
