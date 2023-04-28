package ai.lzy.kafka.s3sink;

import ai.lzy.metrics.MetricReporter;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Singleton
public class S3SinkMetrics {

    private static final String S3SINK = "s3sink";

    public final Gauge activeSessions = Gauge
        .build("active_sessions", "Active S3Sink Sessions")
        .subsystem(S3SINK)
        .register();

    public final Counter uploadedBytes = Counter
        .build("uploaded_bytes", "Uploaded Bytes")
        .subsystem(S3SINK)
        .register();

    public final Counter errors = Counter
        .build("errors", "All Kind of Errors")
        .subsystem(S3SINK)
        .register();

    public S3SinkMetrics(@Named("S3SinkMetricReporter") MetricReporter ignored) {
    }
}
