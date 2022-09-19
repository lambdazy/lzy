package ai.lzy.kharon.workflow;

import ai.lzy.allocator.AllocatorAgent;
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
import ai.lzy.kharon.KharonConfig;
import ai.lzy.kharon.KharonDataSource;
import ai.lzy.kharon.workflow.dao.ExecutionDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.OperationServiceApiGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateMetadata;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse;
import ai.lzy.v1.VmAllocatorApi.CreateSessionRequest;
import ai.lzy.v1.VmAllocatorApi.CreateSessionResponse;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LWFS.*;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static ai.lzy.channelmanager.grpc.ProtoConverter.createChannelRequest;
import static ai.lzy.kharon.KharonConfig.PortalConfig;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class WorkflowService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    public static final String ENV_PORTAL_PKEY = "LZY_PORTAL_PKEY";

    private final PortalConfig portalConfig;
    private final String channelManagerAddress;
    private final JwtCredentials internalUserCredentials;

    private final Duration waitAllocateTimeout;

    private final ExecutionDao dao;
    private final KharonDataSource storage;

    private final ManagedChannel allocatorServiceChannel;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorServiceClient;

    private final ManagedChannel operationServiceChannel;
    private final OperationServiceApiGrpc.OperationServiceApiBlockingStub operationServiceClient;

    private final ManagedChannel storageServiceChannel;
    private final LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient;

    private final ManagedChannel channelManagerChannel;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;

    private final ManagedChannel iamChannel;
    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;

    @Inject
    public WorkflowService(KharonConfig config, KharonDataSource storage, ExecutionDao dao) {
        this.storage = storage;
        this.dao = dao;
        portalConfig = config.getPortal();
        waitAllocateTimeout = config.getWorkflow().getWaitAllocationTimeout();
        channelManagerAddress = config.getChannelManagerAddress();
        internalUserCredentials = config.getIam().createCredentials();

        LOG.info("Init Internal User '{}' credentials", config.getIam().getInternalUserName());

        var allocatorAddress = HostAndPort.fromString(config.getAllocatorAddress());

        allocatorServiceChannel = ChannelBuilder.forAddress(allocatorAddress)
            .usePlaintext()
            .enableRetry(AllocatorGrpc.SERVICE_NAME)
            .build();
        allocatorServiceClient = AllocatorGrpc.newBlockingStub(allocatorServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        operationServiceChannel = ChannelBuilder.forAddress(allocatorAddress)
            .usePlaintext()
            .enableRetry(OperationServiceApiGrpc.SERVICE_NAME)
            .build();
        operationServiceClient = OperationServiceApiGrpc.newBlockingStub(operationServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        storageServiceChannel = ChannelBuilder.forAddress(HostAndPort.fromString(config.getStorage().getAddress()))
            .usePlaintext()
            .enableRetry(LzyStorageServiceGrpc.SERVICE_NAME)
            .build();
        storageServiceClient = LzyStorageServiceGrpc.newBlockingStub(storageServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        channelManagerChannel = ChannelBuilder.forAddress(HostAndPort.fromString(config.getChannelManagerAddress()))
            .usePlaintext()
            .enableRetry(LzyChannelManagerPrivateGrpc.SERVICE_NAME)
            .build();
        channelManagerClient = LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        iamChannel = ChannelBuilder
            .forAddress(config.getIam().getAddress())
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();

        subjectClient = new SubjectServiceGrpcClient(iamChannel, config.getIam()::createCredentials);
        abClient = new AccessBindingServiceGrpcClient(iamChannel, config.getIam()::createCredentials);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        storageServiceChannel.shutdown();
        allocatorServiceChannel.shutdown();
        operationServiceChannel.shutdown();
        channelManagerChannel.shutdown();
        iamChannel.shutdown();
    }

    @Override
    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();
        var workflowName = request.getWorkflowName();
        var executionId = workflowName + "_" + UUID.randomUUID();

        boolean internalSnapshotStorage = !request.hasSnapshotStorage();
        String storageType;
        LMS3.S3Locator storageData;

        BiConsumer<io.grpc.Status, String> replyError = (status, descr) -> {
            LOG.error("[createWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asRuntimeException());
        };

        if (internalSnapshotStorage) {
            storageType = "internal";
            try {
                storageData = createTempStorageBucket(userId);
            } catch (StatusRuntimeException e) {
                replyError.accept(e.getStatus(), "Cannot create internal storage");
                return;
            }
            if (storageData == null) {
                replyError.accept(Status.INTERNAL, "Cannot create internal storage");
                return;
            }
        } else {
            storageType = "user";
            // TODO: ssokolvyak -- move to validator
            if (request.getSnapshotStorage().getEndpointCase() == LMS3.S3Locator.EndpointCase.ENDPOINT_NOT_SET) {
                replyError.accept(Status.INVALID_ARGUMENT, "Snapshot storage not set");
                return;
            }
            storageData = request.getSnapshotStorage();
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                dao.create(executionId, userId, workflowName, storageType, storageData, null));
        } catch (AlreadyExistsException e) {
            if (internalSnapshotStorage) {
                safeDeleteTempStorageBucket(storageData.getBucket());
            }
            replyError.accept(Status.ALREADY_EXISTS, "Cannot create new execution with " +
                "{ workflowName: '%s', userId: '%s' }: %s".formatted(workflowName, userId, e.getMessage()));
            return;
        } catch (Exception e) {
            if (internalSnapshotStorage) {
                safeDeleteTempStorageBucket(storageData.getBucket());
            }
            replyError.accept(Status.INTERNAL, "Cannot create new execution with " +
                "{ workflowName: '%s', userId: '%s' }: %s".formatted(workflowName, userId, e.getMessage()));
            return;
        }

        if (startPortal(workflowName, executionId, userId, response)) {
            LOG.info("Workflow successfully started...");

            var result = CreateWorkflowResponse.newBuilder().setExecutionId(executionId);
            if (internalSnapshotStorage) {
                result.setInternalSnapshotStorage(storageData);
            }
            response.onNext(result.build());
            response.onCompleted();
        }
    }

    @Override
    public void attachWorkflow(AttachWorkflowRequest request, StreamObserver<AttachWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[attachWorkflow], userId={}, request={}.", userId, JsonUtils.printSingleLine(request));

        BiConsumer<io.grpc.Status, String> replyError = (status, descr) -> {
            LOG.error("[attachWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asRuntimeException());
        };

        if (StringUtils.isEmpty(request.getWorkflowName()) || StringUtils.isEmpty(request.getExecutionId())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty 'workflowName' or 'executionId'");
            return;
        }

        try {
            boolean result = withRetries(defaultRetryPolicy(), LOG, () ->
                dao.doesActiveExecutionExists(userId, request.getWorkflowName(), request.getExecutionId()));

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

    @Override
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

        // final String[] bucket = {null};
        // bucket[0] = retrieve from db
        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    dao.updateFinishData(request.getWorkflowName(), request.getExecutionId(),
                        Timestamp.from(Instant.now()), request.getReason(), transaction);
                    dao.updateActiveExecution(userId, request.getWorkflowName(), request.getExecutionId(), null);

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

    private boolean startPortal(String workflowName, String executionId, String userId,
                                StreamObserver<CreateWorkflowResponse> response)
    {
        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                dao.updateStatus(executionId, PortalStatus.CREATING_STD_CHANNELS));

            String[] portalChannelIds = createPortalStdChannels(executionId);
            var stdoutChannelId = portalChannelIds[0];
            var stderrChannelId = portalChannelIds[1];

            withRetries(defaultRetryPolicy(), LOG, () ->
                dao.updateStdChannelIds(executionId, stdoutChannelId, stderrChannelId));

            var sessionId = createSession(userId);

            withRetries(defaultRetryPolicy(), LOG, () -> dao.updateAllocatorSession(executionId, sessionId));

            var startAllocationTime = Instant.now();
            var operation = startAllocation(workflowName, sessionId, executionId, stdoutChannelId, stderrChannelId);
            var opId = operation.getId();

            AllocateMetadata allocateMetadata;
            try {
                allocateMetadata = operation.getMetadata().unpack(AllocateMetadata.class);
            } catch (InvalidProtocolBufferException e) {
                response.onError(Status.INTERNAL
                    .withDescription("Invalid allocate operation metadata: VM id missed. Operation id: " + opId)
                    .asRuntimeException());
                return false;
            }
            var vmId = allocateMetadata.getVmId();

            withRetries(defaultRetryPolicy(), LOG, () -> dao.updateAllocateOperationData(executionId, opId, vmId));

            AllocateResponse allocateResponse = waitAllocation(startAllocationTime.plus(waitAllocateTimeout), opId);
            if (allocateResponse == null) {
                LOG.error("Cannot wait allocate operation response. Operation id: " + opId);
                response.onError(Status.DEADLINE_EXCEEDED.withDescription("Allocation timeout").asRuntimeException());
                return false;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> dao.updateAllocatedVmAddress(executionId,
                allocateResponse.getMetadataOrDefault(AllocatorAgent.VM_IP_ADDRESS, null)));

        } catch (Exception e) {
            response.onError(Status.INTERNAL.withDescription("Cannot save execution data about portal")
                .asRuntimeException());
            return false;
        }
        return true;
    }

    private String[] createPortalStdChannels(String executionId) {
        LOG.info("Creating portal stdout channel with name '{}'", portalConfig.getStdoutChannelName());
        // create portal stdout channel that receives portal output
        String stdoutChannelId = channelManagerClient.create(createChannelRequest(executionId,
            createPortalChannelSpec(portalConfig.getStdoutChannelName()))).getChannelId();

        LOG.info("Creating portal stderr channel with name '{}'", portalConfig.getStderrChannelName());
        // create portal stderr channel that receives portal error output
        String stderrChannelId = channelManagerClient.create(createChannelRequest(executionId,
            createPortalChannelSpec(portalConfig.getStderrChannelName()))).getChannelId();

        return new String[] {stdoutChannelId, stderrChannelId};
    }

    private static LCM.ChannelSpec createPortalChannelSpec(String channelName) {
        return LCM.ChannelSpec.newBuilder()
            .setChannelName(channelName)
            .setContentType(LMD.DataScheme.newBuilder()
                .setSchemeContent("text")
                .setDataFormat("plain")
                .build())
            .setDirect(LCM.DirectChannelType.getDefaultInstance())
            .build();
    }

    public String createSession(String userId) {
        LOG.info("Creating session for user with id '{}'", userId);

        CreateSessionResponse session = allocatorServiceClient.createSession(
            CreateSessionRequest.newBuilder()
                .setOwner(userId)
                .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder().setIdleTimeout(Durations.ZERO))
                .build());
        return session.getSessionId();
    }

    public OperationService.Operation startAllocation(String workflowName, String sessionId, String executionId,
                                                      String stdoutChannelId, String stderrChannelId)
    {
        var portalId = "portal_" + executionId + UUID.randomUUID();

        String privateKey;
        try {
            var workerKeys = RsaUtils.generateRsaKeys();
            var publicKey = Files.readString(workerKeys.publicKeyPath());
            privateKey = Files.readString(workerKeys.privateKeyPath());

            final var subj = subjectClient.createSubject(AuthProvider.INTERNAL, portalId, SubjectType.SERVANT,
                new SubjectCredentials("main", publicKey, CredentialsType.PUBLIC_KEY));

            abClient.setAccessBindings(new Workflow(workflowName),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (Exception e) {
            LOG.error("Cannot build credentials for portal", e);
            throw new RuntimeException(e);
        }

        var args = List.of(
            "-portal.portal-id=" + portalId,
            "-portal.portal-api-port=" + portalConfig.getPortalApiPort(),
            "-portal.fs-api-port=" + portalConfig.getFsApiPort(),
            "-portal.fs-root=" + portalConfig.getFsRoot(),
            "-portal.stdout-channel-id=" + stdoutChannelId,
            "-portal.stderr-channel-id=" + stderrChannelId,
            "-portal.channel-manager-address=" + channelManagerAddress);

        var ports = Map.of(
            portalConfig.getFsApiPort(), portalConfig.getFsApiPort(),
            portalConfig.getPortalApiPort(), portalConfig.getPortalApiPort()
        );

        return allocatorServiceClient.allocate(
            AllocateRequest.newBuilder().setSessionId(sessionId).setPoolLabel("portals")
                .addWorkload(AllocateRequest.Workload.newBuilder()
                    .setName("portal")
                    // TODO: ssokolvyak -- fill the image in production
                    //.setImage(portalConfig.getPortalImage())
                    .addAllArgs(args)
                    .putEnv(ENV_PORTAL_PKEY, privateKey)
                    .putAllPortBindings(ports)
                    .build())
                .build());
    }

    @Nullable
    public AllocateResponse waitAllocation(Instant deadline, String operationId) {
        // TODO: ssokolvyak -- replace on streaming request
        OperationService.Operation allocateOperation;

        while (Instant.now().isBefore(deadline)) {
            allocateOperation = operationServiceClient.get(OperationService.GetOperationRequest.newBuilder()
                .setOperationId(operationId).build());
            if (allocateOperation.getDone()) {
                try {
                    return allocateOperation.getResponse().unpack(AllocateResponse.class);
                } catch (InvalidProtocolBufferException e) {
                    LOG.warn("Cannot deserialize allocate response from operation with id: " + operationId);
                }
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
        }
        return null;
    }

    public LMS3.S3Locator createTempStorageBucket(String userId) {
        var bucket = "tmp-bucket-" + userId;
        LOG.info("Creating new temp storage bucket '{}' for user '{}'", bucket, userId);

        LSS.CreateS3BucketResponse response = storageServiceClient.createS3Bucket(
            LSS.CreateS3BucketRequest.newBuilder()
                .setUserId(userId)
                .setBucket(bucket)
                .build());

        // there something else except AMAZON or AZURE may be returned here?
        return switch (response.getCredentialsCase()) {
            case AMAZON -> LMS3.S3Locator.newBuilder().setAmazon(response.getAmazon()).setBucket(bucket).build();
            case AZURE -> LMS3.S3Locator.newBuilder().setAzure(response.getAzure()).setBucket(bucket).build();
            default -> {
                LOG.error("Unsupported bucket storage type {}", response.getCredentialsCase());
                safeDeleteTempStorageBucket(bucket);
                yield null;
            }
        };
    }

    private void safeDeleteTempStorageBucket(String bucket) {
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

    public enum PortalStatus {
        CREATING_STD_CHANNELS, CREATING_SESSION, REQUEST_VM, ALLOCATING_VM, VM_READY
    }
}
