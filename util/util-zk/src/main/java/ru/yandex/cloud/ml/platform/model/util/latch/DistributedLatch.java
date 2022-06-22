package ru.yandex.cloud.ml.platform.model.util.latch;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedValue;
import org.apache.curator.framework.recipes.shared.VersionedValue;

public class DistributedLatch implements Latch {

    private final SharedCount count;
    private final SharedValue createdAt;

    DistributedLatch(CuratorFramework zookeeperClient, int count, String name) {
        this.count = new SharedCount(zookeeperClient, name + "-count", count);
        this.createdAt = new SharedValue(
            zookeeperClient,
            name + "-createdAt",
            longToBytes(Instant.now().toEpochMilli())
        );

        try {
            this.createdAt.start();
            this.count.start();

            if (this.count.getCount() < 0) {
                this.count.setCount(count);
            }
            if (this.createdAt.getValue().length == 0) {
                this.createdAt.setValue(longToBytes(Instant.now().toEpochMilli()));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // if this counter was created before and then utilised via close() method
    }

    @Override
    public void countDown() {
        while (true) {
            final VersionedValue<Integer> cnt = count.getVersionedValue();
            try {
                if (cnt.getValue() == 0 || count.trySetCount(cnt, cnt.getValue() - 1)) {
                    return;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean await(int timeout, TimeUnit unit) throws InterruptedException {
        long timeoutSecs = unit.toSeconds(timeout);
        while (true) {
            if (count.getCount() == 0) {
                return true;
            }
            if (timeoutSecs <= 0) {
                return false;
            }

            Thread.sleep(1000);
            timeoutSecs--;
        }
    }

    public long createdAt() {
        return bytesToLong(createdAt.getValue());
    }

    public int count() {
        return count.getCount();
    }

    void close() {
        try {
            this.count.setCount(-1);
            this.count.close();
            this.createdAt.setValue(new byte[0]);
            this.createdAt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private long bytesToLong(byte[] bytes) {
        if (bytes.length == 0) {
            return 0;
        }

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }
}
