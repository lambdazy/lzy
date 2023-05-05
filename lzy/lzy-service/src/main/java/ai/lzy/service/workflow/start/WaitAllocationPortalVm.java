package ai.lzy.service.workflow.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.Constants;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class WaitAllocationPortalVm implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final LongRunningServiceBlockingStub allocOpClient;
    private final String execId;
    private final String opId;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public WaitAllocationPortalVm(ExecutionDao execDao, String execId, String allocOpId,
                                  LongRunningServiceBlockingStub allocOpClient,
                                  Function<StatusRuntimeException, StepResult> failAction,
                                  Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.execId = execId;
        this.opId = allocOpId;
        this.allocOpClient = allocOpClient;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Test status of allocate portal VM operation: { opId: {} }", logPrefix, opId);

        final LongRunning.Operation allocateVmOp;
        try {
            allocateVmOp = allocOpClient.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(opId).build());
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in AllocOpService::get call for operation with id='{}': {}", logPrefix, opId,
                sre.getMessage(), sre);

            try {
                //noinspection ResultOfMethodCallIgnored
                allocOpClient.cancel(LongRunning.CancelOperationRequest.newBuilder().setOperationId(opId).build());
            } catch (StatusRuntimeException ex) {
                log.warn("{} Cannot cancel allocate portal VM operation with id='{}' after error '{}': {}", logPrefix,
                    opId, sre.getMessage(), ex.getMessage(), ex);
            }

            return failAction.apply(sre);
        }

        if (!allocateVmOp.getDone()) {
            log.debug("{} Allocate portal VM operation with id='{}' not completed yet, reschedule...", logPrefix, opId);
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        VmAllocatorApi.AllocateResponse allocateResponse;
        if (allocateVmOp.hasResponse()) {
            try {
                allocateResponse = allocateVmOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("{} Cannot parse AllocateResponse from operation with id='{}': {}", logPrefix, opId,
                    e.getMessage(), e);
                return StepResult.RESTART;
            }

            try {
                withRetries(log, () -> execDao.updatePortalVmAddress(execId,
                    allocateResponse.getMetadataOrDefault(Constants.PORTAL_ADDRESS_KEY, null),
                    allocateResponse.getMetadataOrDefault(Constants.FS_ADDRESS_KEY, null),
                    null
                ));
            } catch (Exception e) {
                log.error("{} Cannot save data about allocated portal VM: {}", logPrefix, e.getMessage(), e);

                try {
                    //noinspection ResultOfMethodCallIgnored
                    allocOpClient.cancel(LongRunning.CancelOperationRequest.newBuilder().setOperationId(opId).build());
                } catch (StatusRuntimeException sre) {
                    log.warn("{} Cannot cancel allocate portal VM operation with id='{}' after error {}: ", logPrefix,
                        opId, e.getMessage(), sre);
                }

                return failAction.apply(Status.INTERNAL.withDescription("Cannot allocate portal VM")
                    .asRuntimeException());
            }

            return StepResult.CONTINUE;
        }

        if (allocateVmOp.hasError()) {
            var status = StatusProto.toStatusRuntimeException(allocateVmOp.getError());
            log.error("{} Allocate portal VM operation with id='{}' completed with error: {}", logPrefix, opId,
                status.getMessage());
            return failAction.apply(status);
        }

        assert false;
        return StepResult.CONTINUE;
    }
}
