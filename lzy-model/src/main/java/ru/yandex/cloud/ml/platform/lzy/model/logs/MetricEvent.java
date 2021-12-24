package ru.yandex.cloud.ml.platform.lzy.model.logs;

import java.util.Map;

public class MetricEvent extends BaseEvent {
    private final long millis;

    public MetricEvent(String description, Map<String, String> tags, long millis) {
        super(description, tags);
        this.millis = millis;
    }

    public long getMillis() {
        return millis;
    }
}
