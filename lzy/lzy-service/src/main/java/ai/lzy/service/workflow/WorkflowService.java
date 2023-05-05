package ai.lzy.service.workflow;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.Storage;
import ai.lzy.service.*;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.service.workflow.start.StartExecutionAction;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmPoolServiceApi;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.longrunning.LongRunning;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
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
    private final OperationDao operationDao;
    private final OperationsExecutor executor;
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
    private final LzyServiceConfig config;

    private final KafkaAdminClient kafkaAdminClient;
    private final KafkaLogsListeners kafkaLogsListeners;

    public WorkflowService(LzyServiceConfig config, CleanExecutionCompanion cleanExecutionCompanion,
                           LzyServiceStorage storage, WorkflowDao workflowDao, ExecutionDao executionDao,
                           @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                           @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel,
                           @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                           @Named("IamServiceChannel") ManagedChannel iamChannel,
                           @Named("LzySubjectServiceClient") SubjectServiceGrpcClient subjectClient,
                           @Named("LzyServiceOperationDao") OperationDao operationDao,
                           @Named("LzyServiceOperationsExecutor") OperationsExecutor operationsExecutor,
                           LzyServiceMetrics metrics,
                           KafkaAdminClient kafkaAdminClient, KafkaLogsListeners kafkaLogsListeners)
    {
        allocationTimeout = config.getWaitAllocationTimeout();
        allocatorVmCacheTimeout = config.getAllocatorVmCacheTimeout();
        startupPortalConfig = config.getPortal();
        channelManagerAddress = config.getChannelManagerAddress();
        iamAddress = config.getIam().getAddress();
        whiteboardAddress = config.getWhiteboardAddress();
        this.config = config;
        this.operationDao = operationDao;
        this.executor = operationsExecutor;
        this.metrics = metrics;

        this.storage = storage;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;

        this.cleanExecutionCompanion = cleanExecutionCompanion;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaLogsListeners = kafkaLogsListeners;
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

    public void startWorkflow(Operation op, StartWorkflowRequest request, String StreamObserver<LongRunning.Operation> response) {
        var action = new StartExecutionAction(op.id(), op.description(), storage, operationDao, executor, executionDao, );
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

        executor.startNew(action);

        metrics.activeExecutions.labels(newExecution.getOwner()).inc();

        var portalPort = PEEK_RANDOM_PORTAL_PORTS ? -1 : startupPortalConfig.getPortalApiPort();
        var slotsApiPort = PEEK_RANDOM_PORTAL_PORTS ? -1 : startupPortalConfig.getSlotsApiPort();

        newExecution.startPortal(startupPortalConfig.getDockerImage(), portalPort, slotsApiPort,
            startupPortalConfig.getWorkersPoolSize(), startupPortalConfig.getDownloadsPoolSize(),
            startupPortalConfig.getChunksPoolSize(), startupPortalConfig.getStdoutChannelName(),
            startupPortalConfig.getStderrChannelName(), channelManagerAddress, iamAddress, whiteboardAddress,
            allocationTimeout, allocatorVmCacheTimeout, !config.getKafka().isEnabled());

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

            if (config.getKafka().isEnabled()) {
                var topicDesc = DbHelper.withRetries(LOG, () -> executionDao.getKafkaTopicDesc(executionId, null));

                if (topicDesc != null) {
                    kafkaLogsListeners.listen(request, response, topicDesc);
                    return;
                }
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
