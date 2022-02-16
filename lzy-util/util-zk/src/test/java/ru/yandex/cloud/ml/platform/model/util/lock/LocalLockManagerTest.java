package ru.yandex.cloud.ml.platform.model.util.lock;

import org.junit.Before;

public class LocalLockManagerTest extends LockManagerBaseTest {

    private LockManager lockManager;

    @Before
    public void setUp() {
        lockManager = new LocalLockManager();
    }

    @Override
    LockManager lockManager() {
        return lockManager;
    }
}
