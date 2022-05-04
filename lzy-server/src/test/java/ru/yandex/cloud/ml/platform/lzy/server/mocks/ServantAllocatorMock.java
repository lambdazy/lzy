package ru.yandex.cloud.ml.platform.lzy.server.mocks;

import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocatorBase;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ServantAllocatorMock extends ServantsAllocatorBase {
    private static final int DEFAULT_TIMEOUT_SEC = 5;

    private final List<AllocationRequest> allocationRequests = new ArrayList<>();
    private final CountDownLatch allocationLatch;

    public ServantAllocatorMock(Authenticator auth, int waitBeforeShutdownInSec) {
        this(auth, waitBeforeShutdownInSec, 1);
    }

    public ServantAllocatorMock(Authenticator auth, int waitBeforeShutdownInSec, int expectedAllocations) {
        super(auth, waitBeforeShutdownInSec);
        allocationLatch = new CountDownLatch(expectedAllocations);
    }

    @Override
    protected void requestAllocation(UUID servantId, String servantToken, Provisioning provisioning, String bucket) {
        allocationRequests.add(new AllocationRequest(servantId, servantToken, provisioning, bucket));
        allocationLatch.countDown();
    }

    @Override
    protected void cleanup(ServantConnection s) {

    }

    @Override
    protected void terminate(ServantConnection connection) {

    }

    public boolean waitForAllocations() {
        try {
            return allocationLatch.await(DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Stream<AllocationRequest> allocations() {
        return allocationRequests.stream();
    }

    public static class AllocationRequest {
        private final UUID servantId;
        private final String token;
        private final Provisioning provisioning;
        private final String bucket;

        AllocationRequest(UUID servantId, String token, Provisioning provisioning, String bucket) {
            this.servantId = servantId;
            this.token = token;
            this.provisioning = provisioning;
            this.bucket = bucket;
        }

        public UUID servantId() {
            return servantId;
        }

        public String token() {
            return token;
        }

        public Provisioning provisioning() {
            return provisioning;
        }

        public String bucket() {
            return bucket;
        }
    }
}
