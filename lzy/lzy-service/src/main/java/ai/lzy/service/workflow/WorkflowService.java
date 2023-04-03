package ai.lzy.service.workflow;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.Storage;
import ai.lzy.service.CleanExecutionCompanion;
import ai.lzy.service.PortalSlotsListener;
import ai.lzy.service.LzyServiceMetrics;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmPoolServiceApi;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LWFS.GetAvailablePoolsRequest;
import ai.lzy.v1.workflow.LWFS.GetAvailablePoolsResponse;
import ai.lzy.v1.workflow.LWFS.StartWorkflowRequest;
import ai.lzy.v1.workflow.LWFS.StartWorkflowResponse;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
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
    public static volatile boolean PEEK_RANDOM_PORTAL_PORTS = false;  // Only for tests
    static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final LzyServiceConfig.StartupPortalConfig startupPortalConfig;

    final Storage storage;
    final WorkflowDao workflowDao;

    private final Duration allocationTimeout;
    private final Duration allocatorVmCacheTimeout;
    private final String channelManagerAddress;
    private final String iamAddress;
    private final String whiteboardAddress;
    private final LzyServiceMetrics metrics;

    final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    final LongRunningServiceGrpc.LongRunningServiceBlockingStub allocOpService;
    final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;

    private final VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient;

    final SubjectServiceGrpcClient subjectClient;
    final AccessBindingServiceGrpcClient abClient;

    private final CleanExecutionCompanion cleanExecutionCompanion;
    final ExecutionDao executionDao;
    private final Map<String, Queue<PortalSlotsListener>> listenersByExecution = new ConcurrentHashMap<>();

    public WorkflowService(LzyServiceConfig config, CleanExecutionCompanion cleanExecutionCompanion,
                           LzyServiceStorage storage, WorkflowDao workflowDao, ExecutionDao executionDao,
                           @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                           @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel,
                           @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                           @Named("IamServiceChannel") ManagedChannel iamChannel,
                           @Named("LzySubjectServiceClient") SubjectServiceGrpcClient subjectClient,
                           LzyServiceMetrics metrics)
    {
        allocationTimeout = config.getWaitAllocationTimeout();
        allocatorVmCacheTimeout = config.getAllocatorVmCacheTimeout();
        startupPortalConfig = config.getPortal();
        channelManagerAddress = config.getChannelManagerAddress();
        iamAddress = config.getIam().getAddress();
        whiteboardAddress = config.getWhiteboardAddress();
        this.metrics = metrics;

        this.storage = storage;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;

        this.cleanExecutionCompanion = cleanExecutionCompanion;
        this.allocatorClient = newBlockingClient(
            AllocatorGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());
        this.vmPoolClient = newBlockingClient(
            VmPoolServiceGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());
        this.allocOpService = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());

        this.channelManagerClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP,
            () -> internalUserCredentials.get().token());

        this.subjectClient = subjectClient;
        this.abClient = new AccessBindingServiceGrpcClient(APP, iamChannel, internalUserCredentials::get);
    }

    public void startWorkflow(StartWorkflowRequest request, StreamObserver<StartWorkflowResponse> response) {
        var newExecution = StartExecutionCompanion.of(request, this, startupPortalConfig);
        LOG.info("Start workflow. Create new execution: " + newExecution.getState());
        Consumer<Status> replyError = (status) -> {
            LOG.error("Fail to start new execution: status={}, msg={}.", status,
                status.getDescription() + ", creationState: " + newExecution.getState());
            response.onError(status.asRuntimeException());
        };

        newExecution.checkStorage();
        if (newExecution.isInvalid()) {
            replyError.accept(newExecution.getErrorStatus());
            return;
        }

        var previousActiveExecutionId = newExecution.createExecutionInDao();
        var executionId = newExecution.getExecutionId();

        if (previousActiveExecutionId != null) {
            LOG.info("Attempt to clean previous active execution of workflow: { wfName: {}, prevExId: {} }",
                request.getWorkflowName(), previousActiveExecutionId);

            Status errorStatus = Status.INTERNAL.withDescription("Cancelled by new execution start");
            if (cleanExecutionCompanion.tryToFinishExecution(newExecution.getOwner(), previousActiveExecutionId,
                errorStatus))
            {
                cleanExecutionCompanion.cleanExecution(previousActiveExecutionId);
            }
        }

        if (newExecution.isInvalid()) {
            replyError.accept(newExecution.getErrorStatus());
            return;
        }

        metrics.activeExecutions.labels(newExecution.getOwner()).inc();

        var portalPort = PEEK_RANDOM_PORTAL_PORTS ? -1 : startupPortalConfig.getPortalApiPort();
        var slotsApiPort = PEEK_RANDOM_PORTAL_PORTS ? -1 : startupPortalConfig.getSlotsApiPort();

        newExecution.startPortal(startupPortalConfig.getDockerImage(), portalPort, slotsApiPort,
            startupPortalConfig.getWorkersPoolSize(), startupPortalConfig.getDownloadsPoolSize(),
            startupPortalConfig.getChunksPoolSize(), startupPortalConfig.getStdoutChannelName(),
            startupPortalConfig.getStderrChannelName(), channelManagerAddress, iamAddress, whiteboardAddress,
            allocationTimeout, allocatorVmCacheTimeout);

        if (newExecution.isInvalid()) {
            LOG.info("Attempt to clean invalid execution that not started: { wfName: {}, execId:{} }",
                request.getWorkflowName(), executionId);

            if (cleanExecutionCompanion.tryToFinishWorkflow(newExecution.getOwner(), request.getWorkflowName(),
                executionId, newExecution.getErrorStatus()))
            {
                cleanExecutionCompanion.cleanExecution(executionId);
            }

            replyError.accept(newExecution.getErrorStatus());
            return;
        }

        LOG.info("Workflow started: " + newExecution.getState());
        response.onNext(StartWorkflowResponse.newBuilder().setExecutionId(executionId).build());
        response.onCompleted();
    }

    public void completeExecution(String userId, String executionId, Operation operation) {
        cleanExecutionCompanion.completeExecution(userId, executionId, operation);
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

            var resp = (ServerCallStreamObserver<LWFS.ReadStdSlotsResponse>) response;
            var listener = new PortalSlotsListener(portalDesc.fsAddress(), portalDesc.portalId(), resp);
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
