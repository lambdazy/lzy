package ru.yandex.cloud.ml.platform.model.util.lock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public interface LockManager {

    LockManager withPrefix(String prefix);


    Lock getOrCreate(String key);

    void remove(String key);

    ReadWriteLock readWriteLock();
}
