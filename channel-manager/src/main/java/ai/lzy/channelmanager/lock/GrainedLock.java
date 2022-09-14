package ai.lzy.channelmanager.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GrainedLock {
    private final BucketLock counterLock;
    private final Map<String, Subject> subjectLocks = new ConcurrentHashMap<>();

    public GrainedLock() {
        this.counterLock = new BucketLock(1);
    }

    public GrainedLock(int bucketsCount) {
        this.counterLock = new BucketLock(bucketsCount);
    }

    public void lock(String subjectId) {
        var subject = acquireSubject(subjectId);
        subject.lock.lock();
    }

    public boolean tryLock(String subjectId) {
        var subject = acquireSubject(subjectId);
        if (subject.lock.tryLock()) {
            return true;
        } else {
            releaseSubject(subject);
            return false;
        }
    }

    public void unlock(String subjectId) {
        var subject = subjectLocks.get(subjectId);
        if (subject != null) {
            subject.lock.unlock();
            releaseSubject(subject);
        }
    }

    private Subject acquireSubject(String subjectId) {
        counterLock.lock(subjectId);
        var subject = subjectLocks.computeIfAbsent(subjectId, id -> new Subject(id, new ReentrantLock()));
        subject.refCount.incrementAndGet();
        counterLock.unlock(subjectId);
        return subject;
    }

    private void releaseSubject(Subject subject) {
        counterLock.lock(subject.id);
        if (subject.refCount.decrementAndGet() == 0) {
            subjectLocks.remove(subject.id);
        }
        counterLock.unlock(subject.id);
    }

    protected static class Subject {
        private final String id;
        private final Lock lock;
        private final AtomicInteger refCount;

        protected Subject(String id, Lock lock) {
            this.id = id;
            this.lock = lock;
            refCount = new AtomicInteger(0);
        }
    }

}
