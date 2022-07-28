package ai.lzy.scheduler.exp;

import ai.lzy.model.graph.Provisioning;
import ai.lzy.v1.exp.ServantAllocatorGrpc;
import ai.lzy.v1.exp.ServantAllocatorPrivateGrpc;
import com.google.common.net.HostAndPort;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface ServantAllocatorExp {

    record AllocatedServant(
        String servantId,
        HostAndPort apiEndpoint,
        String fsApiEndpoint,
        Map<String, String> meta
    ) {}

    AllocatedServant allocate(String userId, String executionId, Provisioning provisioning, Duration timeout)
        throws TimeoutException, ServantAllocationException;

    ServantAllocatorGrpc.ServantAllocatorImplBase getPublicGrpcService();

    ServantAllocatorPrivateGrpc.ServantAllocatorPrivateImplBase getPrivateGrpcService();

    class ServantAllocationException extends Exception {
        public ServantAllocationException(String message) {
            super(message);
        }

        public ServantAllocationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
