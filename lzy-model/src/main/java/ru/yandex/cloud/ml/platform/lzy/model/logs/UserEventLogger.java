package ru.yandex.cloud.ml.platform.lzy.model.logs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UserEventLogger {
    private static final Logger LOG = LogManager.getLogger("UserEventLogs");

    public static void log(UserEvent event) {
        LOG.info(String.format("{\"event\": %s }", event.toJson()));
    }
}
