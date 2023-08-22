package ai.lzy.graph.services;

import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS;
import jakarta.annotation.Nullable;

public interface WorkerService {

    // Allocator proxy

    LongRunning.Operation allocateVm(String sessionId, LMO.Requirements requirements, String idempotencyKey);

    void freeVm(String vmId);

    @Nullable
    LongRunning.Operation getAllocOp(String opId);

    @Nullable
    LongRunning.Operation cancelAllocOp(String opId, String reason);


    // IAM proxy

    Subject createWorkerSubject(String vmId, String publicKey, String resourceId);


    // Worker proxy

    void init(String vmId, String userId, String workflowName, String host, int port, String workerPrivateKey);

    @Nullable
    LongRunning.Operation execute(String vmId, LWS.ExecuteRequest request, String idempotencyKey);

    @Nullable
    LongRunning.Operation getWorkerOp(String vmId, String opId);

    @Nullable
    LongRunning.Operation cancelWorkerOp(String vmId, String opId);

    // Restore
    void restoreWorker(String vmId, String host, int port);
}
