package ru.yandex.cloud.ml.platform.model.util.integer;

import java.util.concurrent.atomic.AtomicInteger;

public class LocalSharedInteger implements SharedInteger {
    private final AtomicInteger integer;

    LocalSharedInteger(int value) {integer = new AtomicInteger(value);}


    @Override
    public int get() {
        return integer.get();
    }

    @Override
    public boolean compareAndSet(int oldValue, int newValue) {
        return integer.compareAndSet(oldValue, newValue);
    }

    @Override
    public int inc() {
        return integer.incrementAndGet();
    }

    @Override
    public int dec() {
        return integer.decrementAndGet();
    }
}
