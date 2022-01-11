package ru.yandex.cloud.ml.platform.lzy.model.logs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetricEventLogger {
    private final static Logger LOG = LogManager.getLogger("MetricEventLogs");

    public static void log(MetricEvent event){
        LOG.info(String.format("{\"metric\": %s }", event.toJson()));
    }
}
