package ru.yandex.cloud.ml.platform.model.util.integer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service("LocalSharedIntegerManager")
public class LocalSharedIntegerManager implements SharedIntegerManager {

    private final Map<String, SharedInteger> integers;
    private final String prefix;

    private LocalSharedIntegerManager(Map<String, SharedInteger> integers, String prefix) {
        this.integers = integers;
        this.prefix = prefix;
    }

    @Autowired
    public LocalSharedIntegerManager() {
        this.integers = new ConcurrentHashMap<>();
        this.prefix = "LocalSharedIntegerManager-";
    }

    @Override
    public SharedIntegerManager withPrefix(String prefix) {
        return new LocalSharedIntegerManager(integers, this.prefix + "-" + prefix);
    }

    @Override
    public SharedInteger get(String key, int defaultValue) {
        return integers.computeIfAbsent(this.prefix + "-" + key, k -> new LocalSharedInteger(defaultValue));
    }
}
