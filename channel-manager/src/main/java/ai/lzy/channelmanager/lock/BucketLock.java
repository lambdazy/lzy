package ai.lzy.channelmanager.lock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class BucketLock {
    private final Lock[] locks;
    private final int mask;

    BucketLock(int buckets) {
        if ((buckets & (buckets - 1)) != 0) {
            throw new IllegalArgumentException("Buckets count should be power of 2");
        }
        locks = new Lock[buckets];
        for (int i = 0; i < buckets; ++i) {
            locks[i] = new ReentrantLock();
        }
        mask = buckets - 1;
    }

    void lock(String x) {
        locks[x.hashCode() & mask].lock();
    }

    boolean tryLock(String x) {
        return locks[x.hashCode() & mask].tryLock();
    }

    void unlock(String x) {
        locks[x.hashCode() & mask].unlock();
    }

}
