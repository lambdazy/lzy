package ru.yandex.cloud.ml.platform.model.util.latch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LocalLatch implements Latch {

    private final CountDownLatch latch;

    LocalLatch(int count) {
        this.latch = new CountDownLatch(count);
    }

    @Override
    public void countDown() {
        latch.countDown();
    }

    @Override
    public boolean await(int timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    long getCount() {
        return latch.getCount();
    }
}
