package ai.lzy.model.logs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class MetricEventLogger {
    private static final Logger LOG = LogManager.getLogger("MetricEventLogs");

    public static void log(MetricEvent event) {
        LOG.trace(String.format("{\"metric\": %s}", event.toJson()));
    }

    public static void log(String description, Map<String, String> tags, long millis) {
        log(new MetricEvent(description, tags, millis));
    }

    public static void timeIt(String description, Map<String, String> tags, Runnable runnable) {
        final long start = System.currentTimeMillis();
        runnable.run();
        final long finish = System.currentTimeMillis();
        log(new MetricEvent(description, tags, finish - start));
    }
}
