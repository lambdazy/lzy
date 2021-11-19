package ru.yandex.cloud.ml.platform.model.util.integer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;

public class DistributedSharedInteger implements SharedInteger {
    private final SharedCount sharedCount;

    DistributedSharedInteger(CuratorFramework zookeeperClient, String key, int defaultValue) {
        this.sharedCount = new SharedCount(zookeeperClient, key, defaultValue);
        try {
            this.sharedCount.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int get() {
        return sharedCount.getCount();
    }

    @Override
    public boolean compareAndSet(int oldValue, int newValue) {
        final VersionedValue<Integer> versionedValue = sharedCount.getVersionedValue();
        if (oldValue != versionedValue.getValue()) {
            return false;
        }
        try {
            return sharedCount.trySetCount(versionedValue, newValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int inc() {
        int count = sharedCount.getCount();
        while (!compareAndSet(count, count + 1)) {
            count = sharedCount.getCount();
        }
        return count + 1;
    }

    @Override
    public int dec() {
        int count = sharedCount.getCount();
        while (!compareAndSet(count, count - 1)) {
            count = sharedCount.getCount();
        }
        return count - 1;
    }
}
