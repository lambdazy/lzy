package ai.lzy.service.operations.stop;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalBlockingStub;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class FinishExecutionAction extends OperationRunnerBase {
    private final ExecutionDao execDao;
    private final String execId;
    private final Status finishStatus;
    private final RenewableJwt internalUserCredentials;

    private final String idempotencyKey;
    private final AllocatorBlockingStub allocClient;
    private final LongRunningServiceBlockingStub allocOpClient;
    private final LzyChannelManagerPrivateBlockingStub channelsClient;
    private final LongRunningServiceBlockingStub channelsOpService;
    private final SubjectServiceGrpcClient subjClient;
    private final KafkaAdminClient kafkaClient;
    private final KafkaLogsListeners kafkaLogsListeners;

    private final Function<StatusRuntimeException, StepResult> failAction;

    private PortalDescription portalDesc;
    private LzyPortalBlockingStub portalClient;
    private LongRunningServiceBlockingStub portalOpClient;

    private String finishPortalOpId = null;
    private String deleteAllocSessionOpId = null;
    private String destroyChannelsOpId = null;

    public FinishExecutionAction(String id, String descr, String execId, OperationsExecutor executor, Storage storage,
                                 OperationDao operationsDao, ExecutionDao execDao, Status finishStatus,
                                 RenewableJwt internalUserCredentials, @Nullable String idempotencyKey,
                                 LzyChannelManagerPrivateBlockingStub channelsClient,
                                 LongRunningServiceBlockingStub channelsOpService, AllocatorBlockingStub allocClient,
                                 LongRunningServiceBlockingStub allocOpClient, SubjectServiceGrpcClient subjClient,
                                 KafkaAdminClient kafkaClient, KafkaLogsListeners kafkaLogsListeners)
    {
        super(id, descr, storage, operationsDao, executor);
        this.execDao = execDao;
        this.execId = execId;
        this.finishStatus = finishStatus;
        this.internalUserCredentials = internalUserCredentials;
        this.idempotencyKey = idempotencyKey;
        this.allocClient = allocClient;
        this.allocOpClient = allocOpClient;
        this.channelsClient = channelsClient;
        this.channelsOpService = channelsOpService;
        this.subjClient = subjClient;
        this.kafkaClient = kafkaClient;
        this.kafkaLogsListeners = kafkaLogsListeners;
        this.failAction = sre -> fail(sre.getStatus()) ? StepResult.FINISH : StepResult.RESTART;
    }

    private void setPortalDesc(PortalDescription desc) {
        this.portalDesc = desc;
    }

    private void setPortalClient(LzyPortalBlockingStub portalClient) {
        this.portalClient = portalClient;
    }

    private void setPortalOpClient(LongRunningServiceBlockingStub portalOpClient) {
        this.portalOpClient = portalOpClient;
    }

    private void setFinishPortalOpId(String opId) {
        this.finishPortalOpId = opId;
    }

    private void setDeleteAllocSessionOpId(String opId) {
        this.deleteAllocSessionOpId = opId;
    }

    private void setDestroyChannelsOpId(String opId) {
        this.destroyChannelsOpId = opId;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(finishPortal(), waitFinishPortal(), freePortalVm(),
            deleteAllocSession(), waitDeleteAllocSession(), deletePortalSubject(), destroyChannels(),
            waitDestroyChannels(), deleteKafkaTopic(), this::complete);
    }

    private Supplier<StepResult> preparePortalCleaning() {
        return new PreparePortalCleaning(execDao, execId, internalUserCredentials, this::setPortalDesc,
            this::setPortalClient, this::setPortalOpClient, log(), logPrefix());
    }

    private Supplier<StepResult> finishPortal() {
        var vmAddress = portalDesc != null ? portalDesc.vmAddress().toString() : null;
        return new FinishPortal(vmAddress, idempotencyKey, portalClient, this::setFinishPortalOpId, failAction,
            log(), logPrefix());
    }

    private Supplier<StepResult> waitFinishPortal() {
        return new WaitFinishPortal(execDao, execId, finishPortalOpId, portalOpClient, failAction, log(), logPrefix());
    }

    private Supplier<StepResult> freePortalVm() {
        var vmId = portalDesc != null ? portalDesc.vmId() : null;
        return new FreePortalVm(execDao, execId, vmId, idempotencyKey, allocClient, failAction, log(), logPrefix());
    }

    private Supplier<StepResult> deleteAllocSession() {
        var allocSessionId = portalDesc != null ? portalDesc.allocatorSessionId() : null;
        return new DeleteAllocatorSession(allocSessionId, idempotencyKey, allocClient, this::setDeleteAllocSessionOpId,
            failAction, log(), logPrefix());
    }

    private Supplier<StepResult> waitDeleteAllocSession() {
        return new WaitDeleteAllocatorSession(execDao, execId, deleteAllocSessionOpId, allocOpClient, failAction,
            log(), logPrefix());
    }

    private Supplier<StepResult> deletePortalSubject() {
        var subjectId = portalDesc != null ? portalDesc.subjectId() : null;
        return new DeletePortalSubject(execDao, execId, subjectId, subjClient, failAction, log(), logPrefix());
    }

    private Supplier<StepResult> destroyChannels() {
        return new DestroyChannels(execId, idempotencyKey, channelsClient, this::setDestroyChannelsOpId, failAction,
            log(), logPrefix());
    }

    private Supplier<StepResult> waitDestroyChannels() {
        return new WaitDestroyChannels(destroyChannelsOpId, channelsOpService, failAction, log(), logPrefix());
    }

    private Supplier<StepResult> deleteKafkaTopic() {
        return new DeleteKafkaTopic(execDao, execId, kafkaClient, kafkaLogsListeners, failAction, log(), logPrefix());
    }

    private StepResult complete() {
        var pack = Any.pack(LWFS.FinishWorkflowResponse.getDefaultInstance());
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    execDao.setFinishStatus(execId, finishStatus, tx);
                    completeOperation(null, pack, tx);
                }
            });
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            log().error("{} Cannot complete successful FinishWorkflow op: {}.{}", logPrefix(), e.getMessage(),
                (sqlError ? " Reschedule..." : ""));

            return sqlError ? StepResult.RESTART : StepResult.FINISH;
        }

        return StepResult.FINISH;
    }

    private boolean fail(Status status) {
        log().error("{} Fail FinishWorkflow operation: {}", logPrefix(), status.getDescription());

        try {
            withRetries(log(), () -> failOperation(status, null));
        } catch (OperationCompletedException ex) {
            log().error("{} Cannot fail operation: already completed", logPrefix());
        } catch (NotFoundException ex) {
            log().error("{} Cannot fail operation: not found", logPrefix());
        } catch (Exception ex) {
            log().error("{} Cannot fail operation: {}. Retry later...", logPrefix(), ex.getMessage());
            return false;
        }

        return true;
    }
}
