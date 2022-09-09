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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static ai.lzy.kharon.KharonConfig.PortalConfig;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@SuppressWarnings("UnstableApiUsage")
@Singleton
public class WorkflowService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final PortalConfig portalConfig;
    private final String channelManagerAddress;
    private final JwtCredentials internalUserCredentials;

    private final Duration waitAllocateTimeout;

    private final KharonDataSource db;

    private final ManagedChannel allocatorServiceChannel;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorServiceClient;

    private final ManagedChannel operationServiceChannel;
    private final OperationServiceApiGrpc.OperationServiceApiBlockingStub operationServiceClient;

    private final ManagedChannel storageServiceChannel;
    private final LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient;

    private final ManagedChannel channelManagerChannel;
    private final LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManagerClient;

    @Inject
    public WorkflowService(KharonConfig config, KharonDataSource db) {
        this.db = db;
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
            .enableRetry(LzyChannelManagerGrpc.SERVICE_NAME)
            .build();
        channelManagerClient = LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        storageServiceChannel.shutdown();
        allocatorServiceChannel.shutdown();
        operationServiceChannel.shutdown();
        channelManagerChannel.shutdown();
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
                storageData = createTempStorageBucket(userId);
            } catch (StatusRuntimeException e) {
                response.onError(e.getStatus().withDescription("Cannot create internal storage")
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

    private boolean startPortal(String executionId, String userId, StreamObserver<CreateWorkflowResponse> response) {
        try {
            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> updateDatabase(connection -> {
                    var st = connection.prepareStatement("""
                        UPDATE workflow_executions
                        SET portal = cast(? as portal_status)
                        WHERE execution_id = ?""");
                    st.setString(1, PortalStatus.CREATING_STD_CHANNELS.name());
                    st.setString(2, executionId);
                    return st;
                }));

            String[] portalChannelIds = createPortalStdChannels(executionId);
            var stdoutChannelId = portalChannelIds[0];
            var stderrChannelId = portalChannelIds[1];

            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> updateDatabase(connection -> {
                    var st = connection.prepareStatement("""
                        UPDATE workflow_executions
                        SET portal_stdout_channel_id = ?, portal_stderr_channel_id = ?, 
                            portal = cast(? as portal_status)
                        WHERE execution_id = ?""");
                    st.setString(1, stdoutChannelId);
                    st.setString(2, stderrChannelId);
                    st.setString(3, PortalStatus.CREATING_SESSION.name());
                    st.setString(4, executionId);
                    return st;
                }));

            var sessionId = createSession(userId);

            withRetries(
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
                }));

            var startAllocationTime = Instant.now();
            var operation = startAllocation(sessionId, executionId, stdoutChannelId, stderrChannelId);
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

            withRetries(
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
                }));

            AllocateResponse allocateResponse = waitAllocation(startAllocationTime.plus(waitAllocateTimeout), opId);
            if (allocateResponse == null) {
                LOG.error("Cannot wait allocate operation response. Operation id: " + opId);
                response.onError(Status.DEADLINE_EXCEEDED.withDescription("Allocation timeout").asRuntimeException());
                return false;
            }

            withRetries(
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
                }));

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
        String stdoutChannelId = channelManagerClient.create(ChannelManager.ChannelCreateRequest.newBuilder()
            .setWorkflowId(executionId)
            .setChannelSpec(
                Channels.ChannelSpec.newBuilder()
                    .setChannelName(portalConfig.getStdoutChannelName())
                    .setContentType(Operations.DataScheme.newBuilder()
                        .setType("text")
                        .setSchemeType(Operations.SchemeType.plain)
                        .build())
                    .setDirect(Channels.DirectChannelType.getDefaultInstance())
                    .build()
            ).build()).getChannelId();

        LOG.info("Creating portal stderr channel with name '{}'", portalConfig.getStderrChannelName());
        // create portal stderr channel that receives portal error output
        String stderrChannelId = channelManagerClient.create(ChannelManager.ChannelCreateRequest.newBuilder()
            .setWorkflowId(executionId)
            .setChannelSpec(
                Channels.ChannelSpec.newBuilder()
                    .setChannelName(portalConfig.getStderrChannelName())
                    .setContentType(Operations.DataScheme.newBuilder()
                        .setType("text")
                        .setSchemeType(Operations.SchemeType.plain)
                        .build())
                    .setDirect(Channels.DirectChannelType.getDefaultInstance())
                    .build()
            ).build()).getChannelId();

        return new String[] {stdoutChannelId, stderrChannelId};
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
        LOG.info("Creating session for user with id '{}'", userId);

        CreateSessionResponse session = allocatorServiceClient.createSession(
            CreateSessionRequest.newBuilder()
                .setOwner(userId)
                .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder().setIdleTimeout(Durations.ZERO))
                .build());
        return session.getSessionId();
    }

    public OperationService.Operation startAllocation(String sessionId, String executionId,
                                                      String stdoutChannelId, String stderrChannelId)
    {
        var portalId = "portal_" + executionId + UUID.randomUUID();

        var args = List.of(
            "-portal.portal-id=" + portalId,
            "-portal.portal-api-port=" + portalConfig.getPortalApiPort(),
            "-portal.fs-api-port=" + portalConfig.getFsApiPort(),
            "-portal.fs-root=" + portalConfig.getFsRoot(),
            "-portal.token=" + internalUserCredentials.token(),
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
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
        return null;
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

    public SnapshotStorage createTempStorageBucket(String userId) {
        var bucket = "tmp-bucket-" + userId;
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
        CREATING_STD_CHANNELS, CREATING_SESSION, REQUEST_VM, ALLOCATING_VM, VM_READY
    }
}
