package ru.yandex.cloud.ml.platform.model.util.lock;

import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

public abstract class LockManagerBaseTest {
    abstract LockManager lockManager();

    @Test
    public void testTryLock() throws InterruptedException {
        //Arrange
        final String key = UUID.randomUUID().toString();
        final Lock lock = lockManager().getOrCreate(key);
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        //Act
        new Thread(() -> {
            Objects.requireNonNull(lock).lock();
            countDownLatch.countDown();
        }).start();
        countDownLatch.await();

        //Assert
        Assert.assertFalse(Objects.requireNonNull(lockManager().getOrCreate(key)).tryLock());
    }

    @Test
    public void testTryLockWithPrefix() throws InterruptedException {
        //Arrange
        final String key = UUID.randomUUID().toString();
        final LockManager lockManagerOne = lockManager().withPrefix(UUID.randomUUID().toString());
        final LockManager lockManagerTwo = lockManager().withPrefix(UUID.randomUUID().toString());
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        //Act
        new Thread(() -> {
            lockManagerOne.getOrCreate(key).lock();
            countDownLatch.countDown();
        }).start();
        countDownLatch.await();

        //Assert
        Assert.assertFalse(Objects.requireNonNull(lockManagerOne.getOrCreate(key)).tryLock());
        Assert.assertTrue(Objects.requireNonNull(lockManagerTwo.getOrCreate(key)).tryLock());
    }

    @Test
    public void testRemoveAndTryLock() throws InterruptedException {
        //Arrange
        final String key = UUID.randomUUID().toString();
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        //Act
        new Thread(() -> {
            Objects.requireNonNull(lockManager().getOrCreate(key)).lock();
            lockManager().remove(key);
            countDownLatch.countDown();
        }).start();
        countDownLatch.await();

        //Assert
        Assert.assertTrue(Objects.requireNonNull(lockManager().getOrCreate(key)).tryLock());
    }
}
