package ai.lzy.graph.services;

import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.worker.WorkerApiGrpc;
import com.google.common.net.HostAndPort;

public interface AllocatorService {
    LongRunning.Operation allocate(String userId, String workflowName,
                                   String sessionId, LMO.Requirements requirements);

    String createSession(String userId, String workflowName, String idempotencyKey);

    void free(String vmId);

    VmAllocatorApi.AllocateResponse getResponse(String allocOperationId);

    void addCredentials(String vmId, String publicKey, String resourceId);

    WorkerApiGrpc.WorkerApiBlockingStub createWorker(HostAndPort hostAndPort);
    LongRunningServiceGrpc.LongRunningServiceBlockingStub getWorker(HostAndPort hostAndPort);
}
