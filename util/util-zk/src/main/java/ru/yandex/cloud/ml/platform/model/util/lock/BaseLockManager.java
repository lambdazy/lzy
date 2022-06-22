package ru.yandex.cloud.ml.platform.model.util.lock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

public abstract class BaseLockManager implements LockManager {

    protected final ConcurrentHashMap<String, Lock> locks;

    protected BaseLockManager(ConcurrentHashMap<String, Lock> locks) {
        this.locks = locks;
    }

    protected abstract Lock createLock(String path);

    protected abstract String prefix();

    @Override
    public Lock getOrCreate(String key) {
        return locks.computeIfAbsent(prefix() + "-" + key, this::createLock);
    }

    @Override
    public void remove(String key) {
        final Lock remove = locks.remove(prefix() + "-" + key);
        if (remove != null && !remove.tryLock()) {
            remove.unlock();
        }
    }
}
