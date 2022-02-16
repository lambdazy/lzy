package ru.yandex.cloud.ml.platform.model.util.latch;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Lazy
@Service("DistributedLatchManager")
public class DistributedLatchManager implements LatchManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedLatchManager.class);

    private final Map<String, DistributedLatch> latches;
    private final CuratorFramework zookeeperClient;
    private final String prefix;
    private final int latchTimeoutMins;

    @Autowired
    public DistributedLatchManager(CuratorFramework zookeeperClient, int latchTimeoutMins) {
        this.zookeeperClient = zookeeperClient;
        this.latches = new ConcurrentHashMap<>();
        this.prefix = "/DistributedLatchManager";
        this.latchTimeoutMins = latchTimeoutMins;
    }

    private DistributedLatchManager(CuratorFramework zookeeperClient,
        Map<String, DistributedLatch> latches,
        String prefix,
        int latchTimeoutMins) {
        this.zookeeperClient = zookeeperClient;
        this.latches = latches;
        this.prefix = prefix;
        this.latchTimeoutMins = latchTimeoutMins;
    }

    @Override
    public LatchManager withPrefix(String prefix) {
        return new DistributedLatchManager(zookeeperClient, latches, this.prefix + "-" + prefix, latchTimeoutMins);
    }

    @Override
    public Latch create(String key, int count) {
            {
                final DistributedLatch latch = latches.get(this.prefix + "-" + key);
                if (latch != null && latch.count() >= 0 && !isExpired(key, latch)) {
                    throw new RuntimeException("Cannot create latch for key " + key + ": latch already exists");
                }
            }
        final DistributedLatch latch = new DistributedLatch(zookeeperClient, count, this.prefix + "-" + key);
        latches.put(this.prefix + "-" + key, latch);
        return latch;
    }

    @Nullable
    @Override
    public Latch get(String key) {
        final DistributedLatch latch = latches.computeIfAbsent(
            this.prefix + "-" + key,
            k -> new DistributedLatch(zookeeperClient, -1, this.prefix + "-" + key)
        );
        if (isExpired(key, latch)) {
            return null;
        }
        return latch.count() >= 0 ? latch : null;
    }

    private boolean isExpired(String key, DistributedLatch latch) {
        if (latch.createdAt() + latchTimeoutMins * 60L * 1000 < Instant.now().toEpochMilli()) {
            latches.remove(this.prefix + "-" + key);
            latch.close();

            LOGGER.warn("Latch for key " + key + " expired "
                + (Instant.now().toEpochMilli() - latch.createdAt() - latchTimeoutMins * 60L * 1000) / 1000.0 / 60
                + " minutes ago");
            return true;
        }

        return false;
    }

    @Override
    public void remove(String key) {
        final DistributedLatch latch = latches.get(this.prefix + "-" + key);
        latches.remove(this.prefix + "-" + key);

        if (latch != null) {
            latch.close();
        }
    }
}
