package ai.lzy.service.workflow;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.model.Constants;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.service.PortalSlotsListener;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.PortalStatus;
import ai.lzy.service.data.StorageType;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmPoolServiceApi;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LWFS.*;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.*;

@Singleton
public class WorkflowService {
    public static boolean PEEK_RANDOM_PORTAL_PORTS = false;  // Only for tests
    private static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final LzyServiceConfig.StartupPortalConfig startupPortalConfig;

    private final Storage storage;
    private final WorkflowDao workflowDao;

    private final Duration allocationTimeout;
    private final Duration allocatorVmCacheTimeout;
    private final Duration bucketCreationTimeout;
    private final String channelManagerAddress;
    private final String iamAddress;
    private final String whiteboardAddress;
    private final RenewableJwt interanalCreds;

    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub allocOpService;

    private final LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub storageOpService;

    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;

    private final VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient;

    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;

    private final Map<String, Queue<PortalSlotsListener>> listenersByExecution = new ConcurrentHashMap<>();

    public WorkflowService(LzyServiceConfig config, LzyServiceStorage storage, WorkflowDao workflowDao,
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
        interanalCreds = config.getIam().createRenewableToken();

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

        this.storage = storage;
        this.workflowDao = workflowDao;
    }

    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> response) {
        var creationState = new CreateExecutionState(AuthenticationContext.currentSubject().id(),
            request.getWorkflowName());

        LOG.debug("[createWorkflow], init state: " + creationState);

        Consumer<Status> replyError = (status) -> {
            LOG.error("[createWorkflow], fail: status={}, msg={}.", status,
                status.getDescription() + ", creationState: " + creationState);
            response.onError(status.asRuntimeException());
        };

        setStorage(creationState, request);

        if (creationState.isInvalid()) {
            replyError.accept(creationState.getErrorStatus());
            return;
        }

        createExecutionInDao(creationState);

        if (creationState.isInvalid()) {
            replyError.accept(creationState.getErrorStatus());
            return;
        }

        startPortal(creationState);

        if (creationState.isInvalid()) {
            try {
                withRetries(defaultRetryPolicy(), LOG, () -> {
                    try (var transaction = TransactionHandle.create(storage)) {
                        workflowDao.updateFinishData(creationState.getWorkflowName(), creationState.getExecutionId(),
                            Timestamp.from(Instant.now()), creationState.getErrorStatus().getDescription(),
                            creationState.getErrorStatus().getCode().value(), transaction);
                        workflowDao.updateActiveExecution(creationState.getUserId(), creationState.getWorkflowName(),
                            creationState.getExecutionId(), null, transaction);

                        transaction.commit();
                    }
                });
            } catch (Exception e) {
                LOG.error("[createWorkflow] Got Exception during saving error status: " + e.getMessage(), e);
            }
            replyError.accept(creationState.getErrorStatus());
            return;
        }

        LOG.debug("[createWorkflow], created state: " + creationState);

        response.onNext(LWFS.CreateWorkflowResponse.newBuilder()
            .setExecutionId(creationState.getExecutionId())
            .setInternalSnapshotStorage(creationState.getStorageType() == StorageType.INTERNAL
                ? creationState.getStorageConfig()
                : LMST.StorageConfig.getDefaultInstance())
            .build());
        response.onCompleted();
    }

    public void attachWorkflow(AttachWorkflowRequest request, StreamObserver<AttachWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[attachWorkflow], userId={}, request={}.", userId, JsonUtils.printSingleLine(request));

        BiConsumer<Status, String> replyError = (status, descr) -> {
            LOG.error("[attachWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asRuntimeException());
        };

        if (StringUtils.isEmpty(request.getWorkflowName()) || StringUtils.isEmpty(request.getExecutionId())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty 'workflowName' or 'executionId'");
            return;
        }

        try {
            boolean result = withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.doesActiveExecutionExists(userId, request.getWorkflowName(), request.getExecutionId()));

            if (result) {
                LOG.info("[attachWorkflow] workflow '{}/{}' successfully attached.",
                    request.getWorkflowName(), request.getExecutionId());

                response.onNext(AttachWorkflowResponse.getDefaultInstance());
                response.onCompleted();
            } else {
                replyError.accept(Status.NOT_FOUND, "");
            }
        } catch (Exception e) {
            LOG.error("[attachWorkflow] Got Exception: " + e.getMessage(), e);
            replyError.accept(Status.INTERNAL, "Cannot retrieve data about workflow");
        }
    }

    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<FinishWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[finishWorkflow], uid={}, request={}.", userId, JsonUtils.printSingleLine(request));

        BiConsumer<io.grpc.Status, String> replyError = (status, descr) -> {
            LOG.error("[finishWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asException());
        };

        if (StringUtils.isEmpty(request.getWorkflowName()) || StringUtils.isEmpty(request.getExecutionId())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty 'workflowName' or 'executionId'");
            return;
        }

        finishPortal(request.getExecutionId());

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    workflowDao.updateFinishData(request.getWorkflowName(), request.getExecutionId(),
                        Timestamp.from(Instant.now()), request.getReason(),
                        Status.CANCELLED.getCode().value(), transaction);
                    workflowDao.updateActiveExecution(userId, request.getWorkflowName(), request.getExecutionId(),
                        null, transaction);

                    transaction.commit();
                }
            });
        } catch (Exception e) {
            LOG.error("[finishWorkflow], fail: {}.", e.getMessage(), e);
            replyError.accept(Status.INTERNAL, "Cannot finish workflow with name '" +
                request.getWorkflowName() + "': " + e.getMessage());
            return;
        }

        response.onNext(FinishWorkflowResponse.getDefaultInstance());
        response.onCompleted();
    }

    private void setStorage(CreateExecutionState state, CreateWorkflowRequest request) {
        var internalSnapshotStorage = !request.hasSnapshotStorage();

        state.setStorageType(internalSnapshotStorage);

        if (internalSnapshotStorage) {
            try {
                var bucketName = "tmp-bucket-" + state.getUserId();

                LOG.info("Creating new temp storage bucket '{}' for user '{}'", bucketName, state.getUserId());

                LongRunning.Operation createOp = withIdempotencyKey(storageServiceClient, UUID.randomUUID().toString())
                    .createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
                        .setUserId(state.getUserId())
                        .setBucket(bucketName)
                        .build());

                createOp = awaitOperationDone(storageOpService, createOp.getId(), bucketCreationTimeout);

                if (!createOp.getDone()) {
                    state.fail(Status.DEADLINE_EXCEEDED,
                        "Cannot wait create bucket operation response. Operation id: " + createOp.getId());
                    return;
                }

                if (createOp.hasError()) {
                    var status = createOp.getError();
                    state.fail(Status.fromCodeValue(status.getCode()), "Cannot process create S3 bucket operation: " +
                        "{ operationId: %s }, error: %s".formatted(createOp.getId(), status.getMessage()));
                    return;
                }

                LSS.CreateS3BucketResponse response = createOp.getResponse().unpack(LSS.CreateS3BucketResponse.class);

                var storageConfig = switch (response.getCredentialsCase()) {
                    case S3 -> LMST.StorageConfig.newBuilder().setS3(response.getS3())
                        .setUri(URI.create("s3://" + bucketName).toString())
                        .build();
                    case AZURE -> LMST.StorageConfig.newBuilder().setAzure(response.getAzure())
                        .setUri(URI.create("azure://" + bucketName).toString())
                        .build();
                    default -> {
                        LOG.error("Unsupported bucket storage type {}", response.getCredentialsCase());
                        deleteTempUserBucket(bucketName);
                        yield null;
                    }
                };

                if (storageConfig == null) {
                    state.fail(Status.INTERNAL, "Cannot create temp bucket");
                    return;
                }

                state.setStorageConfig(storageConfig);
            } catch (StatusRuntimeException e) {
                state.fail(e.getStatus(), "Cannot create temp bucket: " + e.getMessage());
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot deserialize create S3 bucket response from operation: " + e.getMessage());
                state.fail(Status.INTERNAL, "Cannot create temp bucket: " + e.getMessage());
            }
        } else {
            var userStorage = request.getSnapshotStorage();
            if (userStorage.getCredentialsCase() == LMST.StorageConfig.CredentialsCase.CREDENTIALS_NOT_SET) {
                state.fail(Status.INVALID_ARGUMENT, "Credentials are not set");
            } else {
                state.setStorageConfig(userStorage);
            }
        }
    }

    private void deleteTempUserBucket(String bucket) {
        if (StringUtils.isEmpty(bucket)) {
            return;
        }

        LOG.info("Deleting temp storage bucket '{}'", bucket);

        try {
            @SuppressWarnings("unused")
            var resp = storageServiceClient.deleteS3Bucket(
                LSS.DeleteS3BucketRequest.newBuilder()
                    .setBucket(bucket)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Can't delete temp bucket '{}': ({}) {}", bucket, e.getStatus(), e.getMessage(), e);
        }
    }

    private void createExecutionInDao(CreateExecutionState state) {
        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.create(state.getExecutionId(), state.getUserId(), state.getWorkflowName(),
                    state.getStorageType().name(), state.getStorageConfig()));
        } catch (AlreadyExistsException e) {
            state.fail(Status.ALREADY_EXISTS, "Cannot create execution: " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Error while creating execution state in dao", e);
            state.fail(Status.INTERNAL, "Cannot create execution: " + e.getMessage());
        }
    }

    private void startPortal(CreateExecutionState state) {
        var userId = state.getUserId();
        var workflowName = state.getWorkflowName();
        var executionId = state.getExecutionId();

        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.updateStatus(executionId, PortalStatus.CREATING_STD_CHANNELS));

            String[] portalChannelIds = createPortalStdChannels(userId, workflowName, executionId);
            var stdoutChannelId = portalChannelIds[0];
            var stderrChannelId = portalChannelIds[1];

            withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.updateStdChannelIds(executionId, stdoutChannelId, stderrChannelId));

            var sessionId = createAllocatorSession(userId, workflowName, executionId);
            if (sessionId == null) {
                state.fail(Status.INTERNAL, "Cannot create allocator session");
                return;
            }

            var portalId = "portal_" + executionId + UUID.randomUUID();

            withRetries(defaultRetryPolicy(), LOG,
                () -> workflowDao.updateAllocatorSession(executionId, sessionId, portalId));

            var allocateVmOp = startAllocation(userId, workflowName, sessionId,
                executionId, stdoutChannelId,
                stderrChannelId, portalId);
            var opId = allocateVmOp.getId();

            VmAllocatorApi.AllocateMetadata allocateMetadata;
            try {
                allocateMetadata = allocateVmOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
            } catch (InvalidProtocolBufferException e) {
                state.fail(Status.INTERNAL,
                    "Invalid allocate operation metadata: VM id missed. Operation id: " + opId);
                return;
            }
            var vmId = allocateMetadata.getVmId();

            withRetries(defaultRetryPolicy(), LOG,
                () -> workflowDao.updateAllocateOperationData(executionId, opId, vmId));

            allocateVmOp = awaitOperationDone(allocOpService, opId, allocationTimeout);

            if (!allocateVmOp.getDone()) {
                state.fail(Status.DEADLINE_EXCEEDED,
                    "Cannot wait allocate operation response. Operation id: " + opId);
                return;
            }

            if (allocateVmOp.hasError()) {
                var status = allocateVmOp.getError();
                state.fail(Status.fromCodeValue(status.getCode()), "Cannot process allocate vm operation: " +
                    "{ operationId: %s }, error: %s".formatted(allocateVmOp.getId(), status.getMessage()));
                return;
            }

            var allocateResponse = allocateVmOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);

            withRetries(defaultRetryPolicy(), LOG, () -> workflowDao.updateAllocatedVmAddress(executionId,
                allocateResponse.getMetadataOrDefault(Constants.PORTAL_ADDRESS_KEY, null),
                allocateResponse.getMetadataOrDefault(Constants.FS_ADDRESS_KEY, null)
            ));

        } catch (StatusRuntimeException e) {
            LOG.error("Cannot start portal", e);
            state.fail(e.getStatus(), "Cannot start portal");
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Cannot deserialize allocate response from operation: " + e.getMessage());
            state.fail(Status.INTERNAL, "Cannot start portal: " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Cannot start portal", e);
            state.fail(Status.INTERNAL, "Cannot start portal: " + e.getMessage());
        }
    }

    private String[] createPortalStdChannels(String userId, String workflowName, String executionId) {
        LOG.info("Creating portal stdout channel with name '{}'", startupPortalConfig.getStdoutChannelName());
        // create portal stdout channel that receives portal output
        var stdoutChannelId = channelManagerClient.create(makeCreateChannelCommand(userId, workflowName, executionId,
            startupPortalConfig.getStdoutChannelName())).getChannelId();

        LOG.info("Creating portal stderr channel with name '{}'", startupPortalConfig.getStderrChannelName());
        // create portal stderr channel that receives portal error output
        var stderrChannelId = channelManagerClient.create(makeCreateChannelCommand(userId, workflowName, executionId,
            startupPortalConfig.getStderrChannelName())).getChannelId();

        return new String[] {stdoutChannelId, stderrChannelId};
    }

    @Nullable
    public String createAllocatorSession(String userId, String workflowName, String executionId) {
        LOG.info("Creating session for {}/{}", userId, workflowName);

        var op = withIdempotencyKey(allocatorClient, executionId)
            .createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setOwner(userId)
                    .setDescription(executionId)
                    .setCachePolicy(
                        VmAllocatorApi.CachePolicy.newBuilder()
                            .setIdleTimeout(Durations.fromSeconds(allocatorVmCacheTimeout.getSeconds()))
                            .build())
                    .build());

        if (op.getDone()) {
            try {
                return op.getResponse().unpack(VmAllocatorApi.CreateSessionResponse.class).getSessionId();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse CreateSessionResponse", e);
                return null;
            }
        }

        LOG.error("Unexpected operation state for {}/{}", userId, workflowName);
        return null;
    }

    public LongRunning.Operation startAllocation(String userId, String workflowName, String sessionId,
                                                 String executionId, String stdoutChannelId, String stderrChannelId,
                                                 String portalId)
    {
        String privateKey;
        try {
            var workerKeys = RsaUtils.generateRsaKeys();
            privateKey = workerKeys.privateKey();

            final var subj = subjectClient.createSubject(AuthProvider.INTERNAL, portalId, SubjectType.WORKER,
                new SubjectCredentials("main", workerKeys.publicKey(), CredentialsType.PUBLIC_KEY));

            abClient.setAccessBindings(new Workflow(userId + "/" + workflowName),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (Exception e) {
            LOG.error("Cannot build credentials for portal, workflow <{}/{}>", userId, workflowName, e);
            throw new RuntimeException(e);
        }

        var portalPort = PEEK_RANDOM_PORTAL_PORTS
            ? FreePortFinder.find(10000, 11000)
            : startupPortalConfig.getPortalApiPort();

        var fsPort = PEEK_RANDOM_PORTAL_PORTS
            ? FreePortFinder.find(11000, 12000)
            : startupPortalConfig.getSlotsApiPort();

        var args = List.of(
            "-portal.portal-id=" + portalId,
            "-portal.portal-api-port=" + portalPort,
            "-portal.slots-api-port=" + fsPort,
            "-portal.stdout-channel-id=" + stdoutChannelId,
            "-portal.stderr-channel-id=" + stderrChannelId,
            "-portal.channel-manager-address=" + channelManagerAddress,
            "-portal.iam-address=" + iamAddress,
            "-portal.whiteboard-address=" + whiteboardAddress);

        var ports = Map.of(
            startupPortalConfig.getSlotsApiPort(), startupPortalConfig.getSlotsApiPort(),
            startupPortalConfig.getPortalApiPort(), startupPortalConfig.getPortalApiPort()
        );

        var portalEnvPKEY = "LZY_PORTAL_PKEY";

        return withIdempotencyKey(allocatorClient, "portal-" + executionId).allocate(
            VmAllocatorApi.AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel(startupPortalConfig.getPoolLabel())
                .setZone(startupPortalConfig.getPoolZone())
                .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.SYSTEM)
                .addWorkload(
                    VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                        .setName("portal")
                        .setImage(startupPortalConfig.getDockerImage())
                        .addAllArgs(args)
                        .putEnv(portalEnvPKEY, privateKey)
                        .putAllPortBindings(ports)
                        .build())
                .build());
    }

    public void readStdSlots(LWFS.ReadStdSlotsRequest request, StreamObserver<LWFS.ReadStdSlotsResponse> response) {
        var executionId = request.getExecutionId();
        try {
            var portalDesc = withRetries(LOG, () -> workflowDao.getPortalDescription(executionId));
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

    private void finishPortal(String executionId) {
        try {
            var portalAddress = withRetries(LOG, () -> workflowDao.getPortalAddress(executionId));
            if (portalAddress == null) {
                LOG.error("Error while building portal channel. Execution id: <{}>", executionId);
                return;
            }

            var portalChannel = newGrpcChannel(portalAddress, LzyPortalGrpc.SERVICE_NAME);

            var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(portalChannel),
                    APP, () -> interanalCreds.get().token());
            var ignored = portalClient.finish(LzyPortalApi.FinishRequest.newBuilder().build());

            portalChannel.shutdown();
            portalChannel.awaitTermination(10, TimeUnit.SECONDS);

        } catch (Exception e) {
            LOG.error("Cannot finish portal for execution <{}>. Please destroy it manually", executionId, e);
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
