package ru.yandex.cloud.ml.platform.model.util.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

@Lazy
@Service("DistributedLockManager")
public class DistributedLockManager extends BaseLockManager {
    private final CuratorFramework zookeeperClient;
    private final String prefix;
    private final int lockTimeoutSec;

    @Autowired
    public DistributedLockManager(
        CuratorFramework zookeeperClient,
        int lockTimeoutSec
    ) {
        super(new ConcurrentHashMap<>());
        this.zookeeperClient = zookeeperClient;
        this.prefix = "/DistributedLockManager";
        this.lockTimeoutSec = lockTimeoutSec;
    }

    private DistributedLockManager(
        CuratorFramework zookeeperClient,
        ConcurrentHashMap<String, Lock> locks,
        String prefix,
        int lockTimeoutSec
    ) {
        super(locks);
        this.zookeeperClient = zookeeperClient;
        this.prefix = prefix;
        this.lockTimeoutSec = lockTimeoutSec;
    }

    @Override
    public LockManager withPrefix(String prefix) {
        return new DistributedLockManager(zookeeperClient, locks, this.prefix + "-" + prefix, lockTimeoutSec);
    }

    @Override
    public ReadWriteLock readWriteLock() {
        final var interProcessReadWriteLock = new InterProcessReadWriteLock(zookeeperClient, prefix);
        final var readLock = new DistributedLock(interProcessReadWriteLock.readLock(), lockTimeoutSec);
        final var writeLock = new DistributedLock(interProcessReadWriteLock.writeLock(), lockTimeoutSec);
        return new ReadWriteLock() {
            @Override
            public Lock readLock() {
                return readLock;
            }

            @Override
            public Lock writeLock() {
                return writeLock;
            }
        };
    }

    @Override
    protected Lock createLock(String path) {
        return new DistributedLock(new InterProcessMutex(zookeeperClient, path), lockTimeoutSec);
    }

    @Override
    protected String prefix() {
        return prefix;
    }

    private static class DistributedLock implements Lock {
        private final InterProcessMutex mutex;
        private final int lockTimeoutSec;

        private DistributedLock(InterProcessMutex mutex, int lockTimeoutSec) {
            this.mutex = mutex;
            this.lockTimeoutSec = lockTimeoutSec;
        }

        @Override
        public void lock() {
            try {
                mutex.acquire();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void lockInterruptibly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            if (mutex.isAcquiredInThisProcess()) {
                return false;
            }

            return tryLock(lockTimeoutSec, TimeUnit.SECONDS);
        }


        @Override
        public boolean tryLock(long time, @Nonnull TimeUnit unit) {

            try {
                return mutex.acquire(time, unit);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void unlock() {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        @Nonnull
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }
}
