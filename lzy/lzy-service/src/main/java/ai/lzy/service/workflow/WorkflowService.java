package ai.lzy.service.workflow;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.ExecutionFinalizer;
import ai.lzy.service.PortalSlotsListener;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.StorageType;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmPoolServiceApi;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LWFS.*;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

@Singleton
public class WorkflowService {
    public static boolean PEEK_RANDOM_PORTAL_PORTS = false;  // Only for tests
    static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final LzyServiceConfig.StartupPortalConfig startupPortalConfig;

    final Storage storage;
    final WorkflowDao workflowDao;

    private final Duration allocationTimeout;
    private final Duration allocatorVmCacheTimeout;
    final Duration bucketCreationTimeout;
    private final String channelManagerAddress;
    private final String iamAddress;
    private final String whiteboardAddress;
    private RenewableJwt internalUserCredentials;

    final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    final LongRunningServiceGrpc.LongRunningServiceBlockingStub allocOpService;

    final LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient;
    final LongRunningServiceGrpc.LongRunningServiceBlockingStub storageOpService;

    final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;

    private final VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient;

    final SubjectServiceGrpcClient subjectClient;
    final AccessBindingServiceGrpcClient abClient;

    private final ExecutionFinalizer executionFinalizer;
    final ExecutionDao executionDao;
    private final Map<String, Queue<PortalSlotsListener>> listenersByExecution = new ConcurrentHashMap<>();

    public WorkflowService(LzyServiceConfig config, ExecutionFinalizer executionFinalizer,
                           LzyServiceStorage storage, WorkflowDao workflowDao, ExecutionDao executionDao,
                           @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                           @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel,
                           @Named("StorageServiceChannel") ManagedChannel storageChannel,
                           @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                           @Named("IamServiceChannel") ManagedChannel iamChannel)
    {
        allocationTimeout = config.getWaitAllocationTimeout();
        allocatorVmCacheTimeout = config.getAllocatorVmCacheTimeout();
        bucketCreationTimeout = config.getStorage().getBucketCreationTimeout();
        startupPortalConfig = config.getPortal();
        channelManagerAddress = config.getChannelManagerAddress();
        iamAddress = config.getIam().getAddress();
        whiteboardAddress = config.getWhiteboardAddress();

        this.storage = storage;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;

        this.executionFinalizer = executionFinalizer;

        this.internalUserCredentials = internalUserCredentials;

        this.allocatorClient = newBlockingClient(
            AllocatorGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());
        this.vmPoolClient = newBlockingClient(
            VmPoolServiceGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());
        this.allocOpService = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());

        this.storageServiceClient = newBlockingClient(
            LzyStorageServiceGrpc.newBlockingStub(storageChannel), APP, () -> internalUserCredentials.get().token());
        this.storageOpService = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(storageChannel), APP, () -> internalUserCredentials.get().token());

        this.channelManagerClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP,
            () -> internalUserCredentials.get().token());

        this.subjectClient = new SubjectServiceGrpcClient(APP, iamChannel, internalUserCredentials::get);
        this.abClient = new AccessBindingServiceGrpcClient(APP, iamChannel, internalUserCredentials::get);
    }

    public void startExecution(StartExecutionRequest request, StreamObserver<StartExecutionResponse> response) {
        var newExecution = StartExecutionCompanion.of(request, this);

        LOG.info("Start new execution: " + newExecution.getState());

        Consumer<Status> replyError = (status) -> {
            LOG.error("Fail to start new execution: status={}, msg={}.", status,
                status.getDescription() + ", creationState: " + newExecution.getState());
            response.onError(status.asRuntimeException());
        };

        newExecution.setStorage();

        if (newExecution.isInvalid()) {
            replyError.accept(newExecution.getErrorStatus());
            return;
        }

        var previousActiveExecutionId = newExecution.createExecutionInDao();

        if (previousActiveExecutionId != null) {
            executionFinalizer.finalizeNow(previousActiveExecutionId);
        }

        if (newExecution.isInvalid()) {
            deactivateExecution(newExecution.getOwner(), request.getWorkflowName(), newExecution.getExecutionId());
            deleteExecution(newExecution.getExecutionId());
            replyError.accept(newExecution.getErrorStatus());
            return;
        }

        var portalPort = PEEK_RANDOM_PORTAL_PORTS ? -1 : startupPortalConfig.getPortalApiPort();
        var slotsApiPort = PEEK_RANDOM_PORTAL_PORTS ? -1 : startupPortalConfig.getSlotsApiPort();

        newExecution.startPortal(startupPortalConfig.getDockerImage(), portalPort, slotsApiPort,
            startupPortalConfig.getStdoutChannelName(), startupPortalConfig.getStderrChannelName(),
            channelManagerAddress, iamAddress, whiteboardAddress, allocationTimeout, allocatorVmCacheTimeout);

        if (newExecution.isInvalid()) {
            deactivateExecution(newExecution.getOwner(), request.getWorkflowName(), newExecution.getExecutionId());
            // todo: delete workflow too in case of created in this execution
            deleteExecution(newExecution.getExecutionId());
            replyError.accept(newExecution.getErrorStatus());
            return;
        }

        LOG.info("New execution started: " + newExecution.getState());

        var storage = newExecution.getState().getStorageType() == StorageType.INTERNAL
            ? newExecution.getState().getStorageLocator()
            : LMS3.S3Locator.getDefaultInstance();

        response.onNext(StartExecutionResponse.newBuilder().setExecutionId(newExecution.getExecutionId())
            .setInternalSnapshotStorage(storage).build());
        response.onCompleted();
    }

    public void finishExecution(FinishExecutionRequest request, StreamObserver<LongRunning.Operation> response) {
        var userId = AuthenticationContext.currentSubject().id();
        var executionId = request.getExecutionId();
        var reason = request.getReason();

        if (StringUtils.isEmpty(executionId)) {
            LOG.error("Cannot finish execution: { executionId: {} }", executionId);
            response.onError(Status.INVALID_ARGUMENT.withDescription("Empty 'executionId'").asRuntimeException());
            return;
        }

        LOG.info("Attempt to finish execution: { userId: {}, executionId: {}, reason: {} }", userId,
            executionId, reason);

        try {
            executionFinalizer.finishInDao(userId, executionId, Status.OK.withDescription(reason));
        } catch (NotFoundException e) {
            LOG.error("Cannot finish execution, not found: { executionId: {}, error: {} }", executionId,
                e.getMessage());
            response.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (IllegalStateException e) {
            LOG.error("Cannot finish execution, invalid state: { executionId: {}, error: {} }", executionId,
                e.getMessage());
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (Exception e) {
            LOG.warn("Unexpected error while finish execution: { executionId: {}, error: {} }", executionId,
                e.getMessage());
        }

        var op = executionFinalizer.finalizeAndAwait(executionId);

        response.onNext(op.toProto());
        response.onCompleted();
    }

    private void deactivateExecution(String userId, String workflowName, String executionId) {
        try {
            withRetries(LOG, () -> workflowDao.setActiveExecutionToNull(userId, workflowName, executionId, null));
        } catch (Exception e) {
            LOG.warn("Cannot deactivate execution of workflow: { workflowName: {}, executionId: {}, error: {} }",
                workflowName, executionId, e.getMessage());
        }
    }

    private void deleteExecution(String executionId) {
        LOG.info("Delete broken execution: { executionId: {} }", executionId);

        PortalDescription portalDesc;
        try {
            portalDesc = withRetries(LOG, () -> executionDao.getPortalDescription(executionId));
        } catch (Exception e) {
            LOG.error("Cannot get portal for execution {}", executionId, e);
            return;
        }

        if (portalDesc != null && portalDesc.vmId() != null) {
            executionFinalizer.freeVm(executionId, portalDesc.vmId());
        }

        if (portalDesc != null && portalDesc.allocatorSessionId() != null) {
            executionFinalizer.deleteSession(executionId, portalDesc.allocatorSessionId());
        }

        try {
            withRetries(LOG, () -> executionDao.delete(executionId, null));
        } catch (Exception e) {
            LOG.error("Cannot delete execution data from dao: { executionId: {} }", executionId, e);
        }
    }

    public void readStdSlots(LWFS.ReadStdSlotsRequest request, StreamObserver<LWFS.ReadStdSlotsResponse> response) {
        var executionId = request.getExecutionId();
        try {
            var portalDesc = withRetries(LOG, () -> executionDao.getPortalDescription(executionId));
            if (portalDesc == null) {
                response.onError(Status.NOT_FOUND.withDescription("Portal not found.").asException());
                return;
            }

            if (portalDesc.portalStatus() != PortalDescription.PortalStatus.VM_READY) {
                response.onError(Status.FAILED_PRECONDITION
                    .withDescription("Portal is creating, retry later.").asException());
                return;
            }

            var listener = new PortalSlotsListener(portalDesc.fsAddress(), portalDesc.portalId(), response);
            listenersByExecution.computeIfAbsent(executionId, k -> new ConcurrentLinkedQueue<>()).add(listener);

        } catch (Exception e) {
            LOG.error("Error while reading std slots: ", e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    public void getAvailablePools(GetAvailablePoolsRequest req, StreamObserver<GetAvailablePoolsResponse> response) {
        var res = vmPoolClient.getVmPools(VmPoolServiceApi.GetVmPoolsRequest.newBuilder()
            .setWithSystemPools(false)
            .setWithUserPools(true)
            .build());

        response.onNext(GetAvailablePoolsResponse.newBuilder()
            .addAllPoolSpecs(
                res.getUserPoolsList().stream()
                    .map(spec -> LWF.VmPoolSpec.newBuilder()
                        .setPoolSpecName(spec.getLabel())
                        .setCpuCount(spec.getCpuCount())
                        .setGpuCount(spec.getGpuCount())
                        .setRamGb(spec.getRamGb())
                        .setCpuType(spec.getCpuType())
                        .setGpuType(spec.getGpuType())
                        .addAllZones(spec.getZonesList())
                        .build())
                    .toList())
            .build());

        response.onCompleted();
    }
}
