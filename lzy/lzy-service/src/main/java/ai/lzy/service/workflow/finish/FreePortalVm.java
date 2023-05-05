package ai.lzy.service.workflow.finish;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class FreePortalVm implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String execId;
    private final String vmId;
    private final String idempotencyKey;
    private final AllocatorBlockingStub allocClient;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public FreePortalVm(ExecutionDao execDao, String execId, @Nullable String vmId, @Nullable String idempotencyKey,
                        AllocatorBlockingStub allocClient, Function<StatusRuntimeException, StepResult> failAction,
                        Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.execId = execId;
        this.vmId = vmId;
        this.idempotencyKey = idempotencyKey;
        this.allocClient = allocClient;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        if (vmId == null) {
            log.debug("{} VM id is null, skip step...", logPrefix);
            return StepResult.ALREADY_DONE;
        }

        log.info("{} Free portal VM: { vmId: {} }", logPrefix, vmId);

        var freeVmAllocClient = (idempotencyKey == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey + "_free_portal_vm");
        try {
            //noinspection ResultOfMethodCallIgnored
            freeVmAllocClient.free(VmAllocatorApi.FreeRequest.newBuilder().setVmId(vmId).build());
            withRetries(log, () -> execDao.updateAllocateOperationData(execId, null, null, null));
        } catch (StatusRuntimeException sre) {
            log.warn("{} Cannot free portal VM: {}", logPrefix, sre.getMessage(), sre);
            return failAction.apply(sre);
        } catch (Exception e) {
            log.warn("{} Cannot clean portal VM id in dao: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot free portal VM").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
