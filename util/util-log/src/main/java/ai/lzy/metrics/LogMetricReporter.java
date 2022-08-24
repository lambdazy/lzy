package ai.lzy.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringWriter;

public final class LogMetricReporter implements MetricReporter {
    private final Logger log;
    private final Level level;

    public LogMetricReporter(String loggerName, Level level) {
        this.log = LogManager.getLogger(loggerName);
        this.level = level;
    }

    public void start() {
    }

    public void stop() {
        log.log(level, getReport());
    }

    private String getReport() {
        try {
            StringWriter writer = new StringWriter();
            TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
