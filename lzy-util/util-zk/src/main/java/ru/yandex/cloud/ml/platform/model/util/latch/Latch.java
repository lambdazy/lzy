package ru.yandex.cloud.ml.platform.model.util.latch;

import java.util.concurrent.TimeUnit;

public interface Latch {

    void countDown();

    boolean await(int timeout, TimeUnit unit) throws InterruptedException;
}
