package ai.lzy.metrics;

import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;

public final class PrometheusMetricReporter implements MetricReporter {
    private final int port;
    private HTTPServer metricsHTTPServer;

    public PrometheusMetricReporter(int port) {
        this.port = port;
    }

    @Override
    public synchronized void start() {
        if (metricsHTTPServer != null) {
            throw new RuntimeException("Prometheus metric reporter already started");
        }
        try {
            metricsHTTPServer = new HTTPServer(port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void stop() {
        if (metricsHTTPServer == null) {
            return;
        }

        metricsHTTPServer.stop();
        metricsHTTPServer = null;
    }
}
