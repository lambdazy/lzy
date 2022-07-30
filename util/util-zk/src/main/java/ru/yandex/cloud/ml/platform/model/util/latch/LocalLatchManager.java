package ru.yandex.cloud.ml.platform.model.util.latch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

public class LocalLatchManager implements LatchManager {

    private final Map<String, LocalLatch> latches;
    private final String prefix;

    public LocalLatchManager() {
        this.latches = new ConcurrentHashMap<>();
        this.prefix = "LocalLatchManager-";
    }

    private LocalLatchManager(Map<String, LocalLatch> latches, String prefix) {
        this.latches = latches;
        this.prefix = prefix;
    }

    @Override
    public LatchManager withPrefix(String prefix) {
        return new LocalLatchManager(latches, this.prefix + "-" + prefix);
    }

    @Override
    public Latch create(String key, int count) {
        if (latches.containsKey(prefix + "-" + key)) {
            throw new RuntimeException("Cannot create latch for key " + key + ": latch already exists");
        }

        LocalLatch latch = new LocalLatch(count);
        latches.put(prefix + "-" + key, latch);
        return latch;
    }

    @Nullable
    @Override
    public Latch get(String key) {
        return latches.get(prefix + "-" + key);
    }

    @Override
    public void remove(String key) {
        final LocalLatch latch = latches.remove(prefix + "-" + key);
        if (latch != null) {
            while (latch.getCount() > 0) {
                latch.countDown();
            }
        }
    }
}
