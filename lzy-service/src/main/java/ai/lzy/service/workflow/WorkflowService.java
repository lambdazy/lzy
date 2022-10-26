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
import ai.lzy.service.PortalSlotsListener;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.PortalStatus;
import ai.lzy.service.data.StorageType;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LWFS.*;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static ai.lzy.channelmanager.grpc.ProtoConverter.makeCreateDirectChannelCommand;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.storage.StorageClient.getOrCreateTempUserBucket;

public class WorkflowService {
    private static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final LzyServiceConfig.StartupPortalConfig startupPortalConfig;

    private final Storage storage;
    private final WorkflowDao workflowDao;

    private final Duration allocationTimeout;
    private final String channelManagerAddress;
    private final String iamAddress;
    private final String whiteboardAddress;

    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceClient;

    private final LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;

    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;

    private final Map<String, Queue<PortalSlotsListener>> listenersByExecution = new ConcurrentHashMap<>();

    public WorkflowService(LzyServiceConfig config, LzyChannelManagerPrivateBlockingStub channelManagerClient,
                           AllocatorGrpc.AllocatorBlockingStub allocatorClient,
                           LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceClient,
                           SubjectServiceGrpcClient subjectClient, AccessBindingServiceGrpcClient abClient,
                           LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient,
                           Storage storage, WorkflowDao workflowDao)
    {
        allocationTimeout = config.getWaitAllocationTimeout();
        startupPortalConfig = config.getPortal();
        channelManagerAddress = config.getChannelManagerAddress();
        iamAddress = config.getIam().getAddress();
        whiteboardAddress = config.getWhiteboardAddress();

        this.allocatorClient = allocatorClient;
        this.operationServiceClient = operationServiceClient;

        this.storageServiceClient = storageServiceClient;
        this.channelManagerClient = channelManagerClient;

        this.subjectClient = subjectClient;
        this.abClient = abClient;

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
            replyError.accept(creationState.getErrorStatus());
            return;
        }

        LOG.debug("[createWorkflow], created state: " + creationState);

        response.onNext(LWFS.CreateWorkflowResponse.newBuilder()
            .setExecutionId(creationState.getExecutionId())
            .setInternalSnapshotStorage(creationState.getStorageType() == StorageType.INTERNAL
                ? creationState.getStorageLocator()
                : LMS3.S3Locator.getDefaultInstance())
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

        for (var listener: listenersByExecution.getOrDefault(request.getExecutionId(), new ConcurrentLinkedQueue<>())) {
            listener.cancel("Workflow <" + request.getExecutionId() + "> is finished");
        }

        // final String[] bucket = {null};
        // bucket[0] = retrieve from db
        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    workflowDao.updateFinishData(request.getWorkflowName(), request.getExecutionId(),
                        Timestamp.from(Instant.now()), request.getReason(), transaction);
                    workflowDao.updateActiveExecution(userId, request.getWorkflowName(), request.getExecutionId(),
                        null);

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

        // TODO: add TTL instead of implicit delete
        // safeDeleteTempStorageBucket(bucket[0]);
    }

    private void setStorage(CreateExecutionState state, CreateWorkflowRequest request) {
        var internalSnapshotStorage = !request.hasSnapshotStorage();

        state.setStorageType(internalSnapshotStorage);

        if (internalSnapshotStorage) {
            try {
                state.setStorageLocator(getOrCreateTempUserBucket(state.getUserId(), storageServiceClient));
            } catch (StatusRuntimeException e) {
                state.fail(e.getStatus(), "Cannot create temp bucket");
            }
        } else {
            var userStorage = request.getSnapshotStorage();
            if (userStorage.getEndpointCase() == LMS3.S3Locator.EndpointCase.ENDPOINT_NOT_SET) {
                state.fail(Status.INVALID_ARGUMENT, "Snapshot storage not set");
            } else {
                state.setStorageLocator(userStorage);
            }
        }
    }

    private void createExecutionInDao(CreateExecutionState state) {
        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.create(state.getExecutionId(), state.getUserId(), state.getWorkflowName(),
                    state.getStorageType().name(), state.getStorageLocator()));
        } catch (AlreadyExistsException e) {
            state.fail(Status.ALREADY_EXISTS, "Cannot create execution: " + e.getMessage());
        } catch (Exception e) {
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

            String[] portalChannelIds = createPortalStdChannels(executionId);
            var stdoutChannelId = portalChannelIds[0];
            var stderrChannelId = portalChannelIds[1];

            withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.updateStdChannelIds(executionId, stdoutChannelId, stderrChannelId));

            var sessionId = createSession(userId);

            var portalId = "portal_" + executionId + UUID.randomUUID();

            withRetries(defaultRetryPolicy(), LOG,
                () -> workflowDao.updateAllocatorSession(executionId, sessionId, portalId));

            var startAllocationTime = Instant.now();
            var operation = startAllocation(userId, workflowName, sessionId,
                executionId, stdoutChannelId,
                stderrChannelId, portalId);
            var opId = operation.getId();

            VmAllocatorApi.AllocateMetadata allocateMetadata;
            try {
                allocateMetadata = operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
            } catch (InvalidProtocolBufferException e) {
                state.fail(Status.INTERNAL,
                    "Invalid allocate operation metadata: VM id missed. Operation id: " + opId);
                return;
            }
            var vmId = allocateMetadata.getVmId();

            withRetries(defaultRetryPolicy(), LOG,
                () -> workflowDao.updateAllocateOperationData(executionId, opId, vmId));

            VmAllocatorApi.AllocateResponse
                allocateResponse = waitAllocation(startAllocationTime.plus(allocationTimeout), opId);
            if (allocateResponse == null) {
                state.fail(Status.DEADLINE_EXCEEDED,
                    "Cannot wait allocate operation response. Operation id: " + opId);
                return;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> workflowDao.updateAllocatedVmAddress(executionId,
                allocateResponse.getMetadataOrDefault(Constants.PORTAL_ADDRESS_KEY, null),
                allocateResponse.getMetadataOrDefault(Constants.FS_ADDRESS_KEY, null)
            ));

        } catch (StatusRuntimeException e) {
            state.fail(e.getStatus(), "Cannot start portal");
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot start portal: " + e.getMessage());
        }
    }

    private String[] createPortalStdChannels(String executionId) {
        LOG.info("Creating portal stdout channel with name '{}'", startupPortalConfig.getStdoutChannelName());
        // create portal stdout channel that receives portal output
        var stdoutChannelId = channelManagerClient.create(makeCreateDirectChannelCommand(executionId,
            startupPortalConfig.getStdoutChannelName())).getChannelId();

        LOG.info("Creating portal stderr channel with name '{}'", startupPortalConfig.getStderrChannelName());
        // create portal stderr channel that receives portal error output
        var stderrChannelId = channelManagerClient.create(makeCreateDirectChannelCommand(executionId,
            startupPortalConfig.getStderrChannelName())).getChannelId();

        return new String[] {stdoutChannelId, stderrChannelId};
    }

    public String createSession(String userId) {
        LOG.info("Creating session for user with id '{}'", userId);

        VmAllocatorApi.CreateSessionResponse session = allocatorClient.createSession(
            VmAllocatorApi.CreateSessionRequest.newBuilder()
                .setOwner(userId)
                .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder().setIdleTimeout(Durations.ZERO))
                .build());
        return session.getSessionId();
    }

    public LongRunning.Operation startAllocation(String userId, String workflowName, String sessionId,
                                                 String executionId, String stdoutChannelId, String stderrChannelId, String portalId)
    {

        String privateKey;
        try {
            var workerKeys = RsaUtils.generateRsaKeys();
            var publicKey = Files.readString(workerKeys.publicKeyPath());
            privateKey = Files.readString(workerKeys.privateKeyPath());

            final var subj = subjectClient.createSubject(AuthProvider.INTERNAL, portalId, SubjectType.SERVANT,
                new SubjectCredentials("main", publicKey, CredentialsType.PUBLIC_KEY));

            abClient.setAccessBindings(new Workflow(userId + "/" + workflowName),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (Exception e) {
            LOG.error("Cannot build credentials for portal", e);
            throw new RuntimeException(e);
        }

        var args = List.of(
            "-portal.portal-id=" + portalId,
            "-portal.portal-api-port=" + startupPortalConfig.getPortalApiPort(),
            "-portal.slots-api-port=" + startupPortalConfig.getSlotsApiPort(),
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

        return allocatorClient.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder().setSessionId(sessionId).setPoolLabel("portals")
                .addWorkload(VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                    .setName("portal")
                    //.setImage(portalConfig.getPortalImage())
                    .addAllArgs(args)
                    .putEnv(portalEnvPKEY, privateKey)
                    .putAllPortBindings(ports)
                    .build())
                .build());
    }

    @Nullable
    public VmAllocatorApi.AllocateResponse waitAllocation(Instant deadline, String operationId) {
        // TODO: ssokolvyak -- replace on streaming request
        LongRunning.Operation allocateOperation;

        while (Instant.now().isBefore(deadline)) {
            allocateOperation = operationServiceClient.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(operationId).build());
            if (allocateOperation.getDone()) {
                try {
                    return allocateOperation.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
                } catch (InvalidProtocolBufferException e) {
                    LOG.warn("Cannot deserialize allocate response from operation with id: " + operationId);
                }
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
        }
        return null;
    }

    public void readStdSlots(
        LWFS.ReadStdSlotsRequest request, StreamObserver<LWFS.ReadStdSlotsResponse> responseObserver)
    {
        var executionId = request.getExecutionId();
        try {
            var portalDesc = withRetries(LOG, () -> workflowDao.getPortalDescription(executionId));
            if (portalDesc == null) {
                throw Status.NOT_FOUND.withDescription("Portal not found").asRuntimeException();
            }

            if (portalDesc.portalStatus() != PortalDescription.PortalStatus.VM_READY) {
                throw Status.UNAVAILABLE.withDescription("Portal is creating, retry later.").asRuntimeException();
            }

            var listener = new PortalSlotsListener(portalDesc.fsAddress(), portalDesc.portalId(), responseObserver);
            listenersByExecution.computeIfAbsent(executionId, k -> new ConcurrentLinkedQueue<>()).add(listener);

        } catch (Exception e) {
            LOG.error("Error while reading slots: ", e);
            throw Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
        }
    }
}
