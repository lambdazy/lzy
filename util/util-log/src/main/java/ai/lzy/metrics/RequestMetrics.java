package ai.lzy.metrics;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

public final class RequestMetrics {
    private final Counter requests;
    private final Counter requestsWithStatus;
    private final Histogram durations;
    private final Counter statuses;
    private final Histogram requestSize;
    private final Histogram responseSize;

    public RequestMetrics(String subsystem) {
        this.requests = Counter
            .build("requests", "Requests meter")
            .subsystem(subsystem)
            .labelNames("app", "method")
            .register();

        this.requestsWithStatus = Counter
            .build("requests_with_status", "Requests meter")
            .subsystem(subsystem)
            .labelNames("app", "method", "status")
            .register();

        this.durations = Histogram
            .build("durations", "Durations histogram (seconds)")
            .subsystem(subsystem)
            .labelNames("app", "method")
            .buckets(0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5,
                10.0, 25.0, 50.0, 75.0, 100.0)
            .register();

        this.statuses = Counter
            .build("statuses", "Statuses counter")
            .subsystem(subsystem)
            .labelNames("app", "method", "status")
            .register();

        this.requestSize = Histogram
            .build("requestSize", "Request package size")
            .subsystem(subsystem)
            .labelNames("app", "method")
            .exponentialBuckets(1024, 2, 10)
            .register();

        this.responseSize = Histogram
            .build("responseSize", "Response package size")
            .subsystem(subsystem)
            .labelNames("app", "method")
            .exponentialBuckets(1024, 2, 10)
            .register();
    }

    public Request begin(String app, String method) {
        return new Request(app, method);
    }

    public final class Request {
        private final String app;
        private final String method;
        private final Histogram.Timer timer;

        private Request(String app, String method) {
            this.app = app;
            this.method = method;
            this.timer = durations.labels(app, method).startTimer();
            requests.labels(app, method).inc();
        }

        public void end(String status, int requestSize, int responseSize) {
            timer.close();

            requestsWithStatus.labels(app, method, status).inc();
            statuses.labels(app, method, status).inc();

            RequestMetrics.this.requestSize.labels(app, method).observe(requestSize);
            RequestMetrics.this.responseSize.labels(app, method).observe(responseSize);
        }
    }
}
