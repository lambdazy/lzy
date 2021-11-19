package ru.yandex.cloud.ml.platform.model.util.integer;

public interface SharedInteger {
    int get();

    boolean compareAndSet(int oldValue, int newValue);

    int inc();

    int dec();
}
