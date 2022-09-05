package ai.lzy.kharon.workflow;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.kharon.KharonConfig;
import ai.lzy.kharon.KharonDataSource;
import ai.lzy.model.db.DaoException;
import ai.lzy.model.db.Transaction;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.*;
import ai.lzy.v1.VmAllocatorApi.*;
import ai.lzy.v1.workflow.LWS.*;
import ai.lzy.v1.workflow.LWSD.SnapshotStorage;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import io.micronaut.core.util.functional.ThrowingFunction;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import static ai.lzy.kharon.KharonConfig.PortalConfig;
import static ai.lzy.model.db.DbHelper.*;

@SuppressWarnings("UnstableApiUsage")
@Singleton
public class WorkflowService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final PortalConfig portalConfig;
    private final String channelManagerAddress;
    private final JwtCredentials internalUserCredentials;

    private final Duration waitAllocateTimeoutMS;

    private final KharonDataSource db;

    private final ManagedChannel storageServiceChannel;
    private final LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient;

    private final ManagedChannel allocatorServiceChannel;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorServiceClient;

    private final ManagedChannel operationServiceChannel;
    private final OperationServiceApiGrpc.OperationServiceApiBlockingStub operationServiceApiBlockingStub;

    @Inject
    public WorkflowService(KharonConfig config, KharonDataSource db) {
        portalConfig = config.getPortal();
        waitAllocateTimeoutMS = Duration.ofMillis(config.getWorkflow().getWaitAllocateTimeoutMS());
        channelManagerAddress = config.getChannelManagerAddress();
        this.db = db;

        internalUserCredentials = config.getIam().createCredentials();
        LOG.info("Init Internal User '{}' credentials", config.getIam().getInternalUserName());

        storageServiceChannel = ChannelBuilder.forAddress(HostAndPort.fromString(config.getStorage().getAddress()))
            .usePlaintext()
            .enableRetry(LzyStorageServiceGrpc.SERVICE_NAME)
            .build();
        storageServiceClient = LzyStorageServiceGrpc.newBlockingStub(storageServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        allocatorServiceChannel = ChannelBuilder.forAddress(HostAndPort.fromString(config.getAllocatorAddress()))
            .usePlaintext()
            .enableRetry(AllocatorGrpc.SERVICE_NAME)
            .build();
        allocatorServiceClient = AllocatorGrpc.newBlockingStub(allocatorServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        operationServiceChannel = ChannelBuilder.forAddress(HostAndPort.fromString(config.getOperationServiceAddress()))
            .usePlaintext()
            .enableRetry(OperationServiceApiGrpc.SERVICE_NAME)
            .build();
        operationServiceApiBlockingStub = OperationServiceApiGrpc.newBlockingStub(operationServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));
    }

    @Override
    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();
        var workflowName = request.getWorkflowName();
        var executionId = workflowName + "_" + UUID.randomUUID();

        boolean internalSnapshotStorage = !request.hasSnapshotStorage();
        String storageType;
        SnapshotStorage storageData;

        if (internalSnapshotStorage) {
            storageType = "internal";
            try {
                storageData = createTempStorageBucket(userId, executionId);
            } catch (StatusRuntimeException e) {
                response.onError(Status.INTERNAL.withDescription("Cannot create internal storage: " + e.getMessage())
                    .asRuntimeException());
                return;
            }
            if (storageData == null) {
                response.onError(Status.INTERNAL.withDescription("Cannot create internal storage")
                    .asRuntimeException());
                return;
            }
        } else {
            storageType = "user";
            // TODO: ssokolvyak -- move to validator
            if (request.getSnapshotStorage().getKindCase() == SnapshotStorage.KindCase.KIND_NOT_SET) {
                response.onError(Status.INVALID_ARGUMENT.withDescription("Snapshot storage not set")
                    .asRuntimeException());
                return;
            }
            storageData = request.getSnapshotStorage();
        }

        boolean doesExecutionCreated;
        try {
            doesExecutionCreated = Transaction.execute(db, connection ->
                createExecution(connection, executionId, userId, workflowName, storageType, storageData, response)
            );
        } catch (DaoException e) {
            var desc = "Cannot create execution: " + e.getMessage();
            LOG.error(desc, e);
            response.onError(Status.INTERNAL.withDescription(desc).asRuntimeException());
            return;
        }

        if (!doesExecutionCreated) {
            if (internalSnapshotStorage) {
                safeDeleteTempStorageBucket(storageData.getBucket());
            }
            return;
        }

        if (startPortal(executionId, userId, response)) {
            var result = CreateWorkflowResponse.newBuilder().setExecutionId(executionId);
            if (internalSnapshotStorage) {
                result.setInternalSnapshotStorage(storageData);
            }
            response.onNext(result.build());
            response.onCompleted();
        }
    }

    private boolean startPortal(String executionId, String userId, StreamObserver<CreateWorkflowResponse> response) {
        boolean updated;

        updated = withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> updateDatabase(connection -> {
                var st = connection.prepareStatement("""
                    UPDATE workflow_executions
                    SET portal = cast(? as portal_status)
                    WHERE execution_id = ?""");
                st.setString(1, PortalStatus.CREATING_SESSION.name());
                st.setString(2, executionId);
                return st;
            }),
            ok -> {
            },
            error -> LOG.error("Cannot set initial portal status in database", error)
        );

        if (!updated) {
            response.onError(Status.INTERNAL.withDescription("Cannot save portal execution data").asRuntimeException());
            return false;
        }

        var sessionId = createSession(userId);

        updated = withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> updateDatabase(connection -> {
                var st = connection.prepareStatement("""
                    UPDATE workflow_executions
                    SET portal = cast(? as portal_status), allocator_session_id = ?
                    WHERE execution_id = ?""");
                st.setString(1, PortalStatus.REQUEST_VM.name());
                st.setString(2, sessionId);
                st.setString(3, executionId);
                return st;
            }),
            ok -> {
            },
            error -> LOG.error("Cannot save session id in database", error)
        );

        if (!updated) {
            response.onError(Status.INTERNAL.withDescription("Cannot save portal execution data").asRuntimeException());
            return false;
        }

        var startAllocationTime = Instant.now();
        var operation = startAllocation(sessionId, executionId);
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

        updated = withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> updateDatabase(connection -> {
                var st = connection.prepareStatement("""
                    UPDATE workflow_executions
                    SET portal = cast(? as portal_status), allocate_op_id = ?, portal_vm_id = ?
                    WHERE execution_id = ?""");
                st.setString(1, PortalStatus.ALLOCATING_VM.name());
                st.setString(2, opId);
                st.setString(3, vmId);
                st.setString(4, executionId);
                return st;
            }),
            ok -> {
            },
            error -> LOG.error("Cannot save allocation operation id and vm id in database", error)
        );

        if (!updated) {
            response.onError(Status.INTERNAL.withDescription("Cannot save portal execution data").asRuntimeException());
            return false;
        }

        AllocateResponse allocateResponse = waitAllocation(startAllocationTime.plus(waitAllocateTimeoutMS), opId);
        if (allocateResponse == null) {
            LOG.error("Cannot wait allocate operation response. Operation id: " + opId);
            response.onError(Status.DEADLINE_EXCEEDED.withDescription("Allocation timeout").asRuntimeException());
            return false;
        }

        updated = withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> updateDatabase(connection -> {
                var st = connection.prepareStatement("""
                    UPDATE workflow_executions
                    SET portal = cast(? as portal_status), portal_vm_address = ?
                    WHERE execution_id = ?""");
                st.setString(1, PortalStatus.VM_READY.name());
                st.setString(2, allocateResponse.getMetadataOrDefault(AllocatorAgent.VM_IP_ADDRESS, null));
                st.setString(3, executionId);
                return st;
            }),
            ok -> {
            },
            error -> LOG.error("Cannot save portal vm address in database", error)
        );

        if (!updated) {
            response.onError(Status.INTERNAL.withDescription("Cannot save portal execution data").asRuntimeException());
        }

        return updated;
    }

    private boolean createExecution(Connection conn, String executionId, String userId,
                                    String workflowName, String storageType, SnapshotStorage storageData,
                                    StreamObserver<CreateWorkflowResponse> response) throws SQLException, IOException
    {
        var st = conn.prepareStatement("""
                SELECT active_execution_id
                FROM workflows
                WHERE user_id = ? AND workflow_name = ?
                FOR UPDATE""",        // TODO: add `nowait` and handle it's warning or error
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        st.setString(1, userId);
        st.setString(2, workflowName);

        var rs = st.executeQuery();
        boolean update = false;
        if (rs.next()) {
            var existingExecutionId = rs.getString("active_execution_id");
            if (StringUtils.isNotEmpty(existingExecutionId)) {
                var rc = Status.ALREADY_EXISTS;
                var desc = String.format("Attempt to start one more instance of workflow: active is '%s'",
                    existingExecutionId);
                LOG.error("[createWorkflow], fail: rc={}, msg={}.", rc, desc);
                response.onError(rc.withDescription(desc).asException());
                return false;
            }
            update = true;
        }

        var st2 = conn.prepareStatement("""
            INSERT INTO workflow_executions
            (execution_id, created_at, storage, storage_bucket, storage_credentials)
            VALUES (?, ?, cast(? as storage_type), ?, ?)""");
        st2.setString(1, executionId);
        st2.setTimestamp(2, Timestamp.from(Instant.now()));
        st2.setString(3, storageType.toUpperCase(Locale.ROOT));
        st2.setString(4, storageData.getBucket());
        var out = new ByteArrayOutputStream(4096);
        storageData.writeTo(out);
        st2.setString(5, out.toString(StandardCharsets.UTF_8));
        st2.executeUpdate();

        if (update) {
            rs.updateString("active_execution_id", executionId);
            rs.updateRow();
        } else {
            st = conn.prepareStatement("""
                INSERT INTO workflows (user_id, workflow_name, created_at, active_execution_id)
                VALUES (?, ?, ?, ?)""");

            st.setString(1, userId);
            st.setString(2, workflowName);
            st.setTimestamp(3, Timestamp.from(Instant.now()));
            st.setString(4, executionId);
            st.executeUpdate();
        }

        LOG.info("[createWorkflow], new execution '{}' for workflow '{}' created.",
            workflowName, executionId);

        return true;
    }

    public String createSession(String userId) {
        CreateSessionResponse session = allocatorServiceClient.createSession(
            CreateSessionRequest.newBuilder()
                .setOwner(userId)
                .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder().setIdleTimeout(Durations.ZERO))
                .build());
        return session.getSessionId();
    }

    public OperationService.Operation startAllocation(String sessionId, String executionId) {
        var portalId = "portal_" + executionId + UUID.randomUUID();

        var envs = Map.of(
            "PORTAL_ID", portalId,
            "HOST", portalConfig.getHost(),
            "PORTAL_API_PORT", portalConfig.getPortalApiPort().toString(),
            "TOKEN", internalUserCredentials.token(),
            "STDOUT_CHANNEL_ID", portalConfig.getStdoutChannelId(),
            "STDERR_CHANNEL_ID", portalConfig.getStderrChannelId(),
            "FS_API_PORT", portalConfig.getFsApiPort().toString(),
            "FS_ROOT", portalConfig.getFsRoot(),
            "CHANNEL_MANAGER_ADDRESS", channelManagerAddress
        );

        var ports = Map.of(
            portalConfig.getFsApiPort(), portalConfig.getFsApiPort(),
            portalConfig.getPortalApiPort(), portalConfig.getPortalApiPort()
        );

        return allocatorServiceClient.allocate(
            AllocateRequest.newBuilder().setSessionId(sessionId).setPoolLabel("portal")
                .addWorkload(AllocateRequest.Workload.newBuilder()
                    .setName("portal")
                    .setImage(portalConfig.getPortalImage())
                    .putAllEnv(envs)
                    .putAllPortBindings(ports)
                    .build())
                .build());
    }

    @Nullable
    public AllocateResponse waitAllocation(Instant deadline, String operationId) {
        // TODO: ssokolvyak -- replace on streaming request
        var request = OperationService.GetOperationRequest.newBuilder().setOperationId(operationId).build();
        OperationService.Operation allocateOperation;

        while (Instant.now().isBefore(deadline)) {
            allocateOperation = operationServiceApiBlockingStub.get(request);
            if (allocateOperation.getDone()) {
                try {
                    return allocateOperation.getResponse().unpack(AllocateResponse.class);
                } catch (InvalidProtocolBufferException e) {
                    LOG.warn("Cannot deserialize allocate response from operation with id: " + operationId);
                }
            }
        }
        return null;
    }

    @Override
    public void attachWorkflow(AttachWorkflowRequest request, StreamObserver<AttachWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[attachWorkflow], userId={}, request={}.", userId, JsonUtils.printSingleLine(request));

        BiConsumer<io.grpc.Status, String> replyError = (status, descr) -> {
            LOG.error("[attachWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asException());
        };

        if (StringUtils.isEmpty(request.getWorkflowName()) || StringUtils.isEmpty(request.getExecutionId())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty 'workflowName' or 'executionId'");
            return;
        }

        try (var conn = db.connect()) {
            var st = conn.prepareStatement("""
                SELECT count(*)
                FROM workflows
                WHERE user_id = ? AND workflow_name = ? AND active_execution_id = ?""");
            st.setString(1, userId);
            st.setString(2, request.getWorkflowName());
            st.setString(2, request.getExecutionId());

            var rs = st.executeQuery();
            if (rs.next()) {
                LOG.info("[attachWorkflow] workflow '{}/{}' successfully attached.",
                    request.getWorkflowName(), request.getExecutionId());

                response.onNext(AttachWorkflowResponse.getDefaultInstance());
                response.onCompleted();
            } else {
                replyError.accept(Status.NOT_FOUND, "");
            }
        } catch (SQLException e) {
            LOG.error("[attachWorkflow] Got SQLException: " + e.getMessage(), e);
            response.onError(e);
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

        try {
            final String[] bucket = {null};

            var success = Transaction.execute(db, conn -> {
                var st = conn.prepareStatement("""
                        SELECT execution_id, finished_at, finished_with_error, storage_bucket
                        FROM workflow_executions
                        WHERE execution_id = ?
                        FOR UPDATE""",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                st.setString(1, request.getExecutionId());

                var rs = st.executeQuery();
                if (rs.next()) {
                    if (rs.getTimestamp("finished_at") != null) {
                        LOG.warn("Attempt to finish already finished workflow '{}' ('{}'). "
                                + "Finished at '{}' with reason '{}'",
                            request.getExecutionId(), request.getWorkflowName(),
                            rs.getTimestamp("finished_at"), rs.getString("finished_with_error"));

                        response.onError(Status.INVALID_ARGUMENT.withDescription("Already finished.").asException());
                        return false;
                    }

                    bucket[0] = rs.getString("storage_bucket");

                    rs.updateTimestamp("finished_at", Timestamp.from(Instant.now()));
                    rs.updateString("finished_with_error", request.getReason());
                    rs.updateRow();
                } else {
                    LOG.warn("Attempt to finish unknown workflow '{}' ('{}') by user '{}'",
                        request.getExecutionId(), request.getWorkflowName(), userId);

                    response.onError(Status.NOT_FOUND.asException());
                    return false;
                }

                var st2 = conn.prepareStatement("""
                    UPDATE workflows
                    SET active_execution_id = NULL
                    WHERE user_id = ? AND workflow_name=? AND active_execution_id = ?""");
                st2.setString(1, userId);
                st2.setString(2, request.getWorkflowName());
                st2.setString(3, request.getExecutionId());
                st2.executeUpdate();

                return true;
            });

            if (success) {
                response.onNext(FinishWorkflowResponse.getDefaultInstance());
                response.onCompleted();

                // TODO: add TTL instead of implicit delete
                // safeDeleteTempStorageBucket(bucket[0]);
            }
        } catch (Exception e) {
            LOG.error("[finishWorkflow], fail: {}.", e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        storageServiceChannel.shutdown();
        allocatorServiceChannel.shutdown();
        operationServiceChannel.shutdown();
    }

    public int updateDatabase(ThrowingFunction<Connection, PreparedStatement, SQLException> function)
        throws SQLException
    {
        try (var connection = db.connect()) {
            var rs = function.apply(connection).executeUpdate();
            if (rs < 1) {
                throw new SQLException("Database UPDATE query changed no one row");
            }
            return rs;
        }
    }

    public SnapshotStorage createTempStorageBucket(String userId, String executionId) {
        var bucket = "tmp__user_" + userId + "__" + executionId;
        LOG.info("Creating new temp storage bucket '{}' for user '{}'", bucket, userId);

        LSS.CreateS3BucketResponse response = storageServiceClient.createS3Bucket(
            LSS.CreateS3BucketRequest.newBuilder()
                .setUserId(userId)
                .setBucket(bucket)
                .build());

        // there something else except AMAZON or AZURE may be returned here?
        return switch (response.getCredentialsCase()) {
            case AMAZON -> SnapshotStorage.newBuilder().setAmazon(response.getAmazon()).setBucket(bucket).build();
            case AZURE -> SnapshotStorage.newBuilder().setAzure(response.getAzure()).setBucket(bucket).build();
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
        CREATING_SESSION, REQUEST_VM, ALLOCATING_VM, VM_READY
    }
}
