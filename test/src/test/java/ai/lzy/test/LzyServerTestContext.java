package ai.lzy.test;

import ai.lzy.priv.v2.LzyServerGrpc;

public interface LzyServerTestContext extends AutoCloseable {
    String address();

    LzyServerGrpc.LzyServerBlockingStub client();

    void init();

    void close();

    enum LocalServantAllocatorType {
        THREAD_ALLOCATOR,
        DOCKER_ALLOCATOR,
        ;
    }
}
