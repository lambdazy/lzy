package ru.yandex.cloud.ml.platform.model.util.lock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LocalLockManager extends BaseLockManager {

    private final String prefix;
    private final ConcurrentHashMap<String, ReadWriteLock> prefixReadWriteLock = new ConcurrentHashMap<>();

    public LocalLockManager() {
        super(new ConcurrentHashMap<>());
        this.prefix = "LocalLockManager";
    }

    private LocalLockManager(String prefix, ConcurrentHashMap<String, Lock> locks) {
        super(locks);
        this.prefix = prefix;
    }

    @Override
    public LockManager withPrefix(String prefix) {
        return new LocalLockManager(this.prefix + "-" + prefix, locks);
    }

    @Override
    public ReadWriteLock readWriteLock() {
        return prefixReadWriteLock.computeIfAbsent(prefix, __ -> new ReentrantReadWriteLock());
    }

    @Override
    protected Lock createLock(String path) {
        return new ReentrantLock();
    }

    @Override
    protected String prefix() {
        return prefix;
    }
}
