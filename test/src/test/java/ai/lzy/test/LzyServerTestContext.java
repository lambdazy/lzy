package ai.lzy.test;

import ai.lzy.v1.deprecated.LzyServerGrpc;

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
