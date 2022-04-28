package ru.yandex.cloud.ml.platform.lzy.model.logs;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetricEventLogger {
    private static final Logger LOG = LogManager.getLogger("MetricEventLogs");

    public static void log(MetricEvent event) {
        LOG.trace(String.format("{\"metric\": %s }", event.toJson()));
    }

    public static void timeIt(String description, Map<String, String> tags, Runnable runnable) {
        final long start = System.currentTimeMillis();
        runnable.run();
        final long finish = System.currentTimeMillis();
        log(new MetricEvent(description, tags, finish - start));
    }
}
