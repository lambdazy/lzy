package ai.lzy.kharon.workflow;

import ai.lzy.iam.authorization.credentials.JwtCredentials;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.kharon.KharonConfig;
import ai.lzy.kharon.KharonDataSource;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.db.Transaction;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.LzyStorageApi;
import ai.lzy.v1.LzyStorageGrpc;
import ai.lzy.v1.LzyWorkflowApi.*;
import ai.lzy.v1.LzyWorkflowGrpc.LzyWorkflowImplBase;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.function.BiConsumer;

import static ai.lzy.model.utils.JwtCredentials.buildJWT;

@SuppressWarnings("UnstableApiUsage")
@Singleton
public class WorkflowService extends LzyWorkflowImplBase {
    private static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final KharonDataSource db;
    private final ManagedChannel storageServiceChannel;
    private final LzyStorageGrpc.LzyStorageBlockingStub storageServiceClient;

    @Inject
    public WorkflowService(KharonConfig config, KharonDataSource db) {
        this.db = db;

        JwtCredentials internalUser;
        try (final Reader reader = new StringReader(config.iam().internal().credentialPrivateKey())) {
            internalUser = new JwtCredentials(buildJWT(config.iam().internal().userName(), reader));
            LOG.info("Init Internal User '{}' credentials", config.iam().internal().userName());
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Cannot build credentials: " + e.getMessage(), e);
        }

        storageServiceChannel = ChannelBuilder.forAddress(HostAndPort.fromString(config.storage().address()))
            .usePlaintext()
            .enableRetry(LzyStorageGrpc.SERVICE_NAME)
            .build();
        storageServiceClient = LzyStorageGrpc.newBlockingStub(storageServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, internalUser::token));
    }

    @Override
    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[createWorkflow], uid={}, request={}.", userId, JsonUtils.printSingleLine(request));

        BiConsumer<io.grpc.Status, String> replyError = (rc, descr) -> {
            LOG.error("[createWorkflow], fail: rc={}, msg={}.", rc, descr);
            response.onError(rc.withDescription(descr).asException());
        };

        if (isNullOrEmpty(request.getWorkflowName())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty `workflowName`");
            return;
        }

        try {
            final String[] executionId = {null};
            final String[] bucket = {null};
            final LzyStorageApi.CreateS3BucketResponse[] bucketCredentials = {null};

            var success = Transaction.execute(db, conn -> {
                var st = conn.prepareStatement("""
                    SELECT active_execution_id
                    FROM workflows
                    WHERE user_id = ? AND workflow_name = ?
                    FOR UPDATE""",        // TODO: add `nowait` and handle it's warning or error
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                st.setString(1, userId);
                st.setString(2, request.getWorkflowName());

                var rs = st.executeQuery();
                boolean update = false;
                if (rs.next()) {
                    var existingExecutionId = rs.getString("active_execution_id");
                    if (!isNullOrEmpty(existingExecutionId)) {
                        replyError.accept(Status.ALREADY_EXISTS, String.format(
                            "Attempt to start one more instance of workflow: active is '%s'", existingExecutionId));
                        return false;
                    }
                    update = true;
                }

                executionId[0] = request.getWorkflowName() + "_" + UUID.randomUUID();

                bucket[0] = "tmp__user_" + userId + "__" + executionId[0];
                bucketCredentials[0] = createTempStorageBucket(userId, bucket[0]);

                if (bucketCredentials[0] == null) {
                    replyError.accept(Status.INTERNAL,
                        "Error while creating workflow temporary storage. Try again later, please.");
                    return false;
                }

                switch (bucketCredentials[0].getCredentialsCase()) {
                    case AMAZON, AZURE -> {
                        // ok
                    }
                    default -> {
                        LOG.error("Unsupported bucket storage type {}", bucketCredentials[0].getCredentialsCase());
                        safeDeleteTempStorageBucket(bucket[0]);
                        return false;
                    }
                }

                var st2 = conn.prepareStatement("""
                    INSERT INTO workflow_executions (execution_id, created_at, storage_bucket)
                    VALUES (?, ?, ?)""");
                st2.setString(1, executionId[0]);
                st2.setTimestamp(2, Timestamp.from(Instant.now()));
                st2.setString(3, bucket[0]);
                st2.executeUpdate();

                if (update) {
                    rs.updateString("active_execution_id", executionId[0]);
                    rs.updateRow();
                } else {
                    st = conn.prepareStatement("""
                        INSERT INTO workflows (user_id, workflow_name, created_at, active_execution_id)
                        VALUES (?, ?, ?, ?)""");

                    st.setString(1, userId);
                    st.setString(2, request.getWorkflowName());
                    st.setTimestamp(3, Timestamp.from(Instant.now()));
                    st.setString(4, executionId[0]);
                    st.executeUpdate();
                }

                LOG.info("[createWorkflow], new execution '{}' for workflow '{}' created.",
                    request.getWorkflowName(), executionId[0]);

                return true;
            });

            if (success) {
                var respBuilder = CreateWorkflowResponse.TempStorage.newBuilder()
                    .setBucket(bucket[0]);

                switch (bucketCredentials[0].getCredentialsCase()) {
                    case AMAZON -> respBuilder.setAmazon(bucketCredentials[0].getAmazon());
                    case AZURE -> respBuilder.setAzure(bucketCredentials[0].getAzure());
                    default -> throw new AssertionError("" + bucketCredentials[0].getCredentialsCase());
                }

                response.onNext(CreateWorkflowResponse.newBuilder()
                    .setExecutionId(executionId[0])
                    .setTempStorage(respBuilder.build())
                    .build());
                response.onCompleted();
            }
        } catch (Exception e) {
            LOG.error("[createWorkflow] Got exception: " + e.getMessage(), e);
            response.onError(e);
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

        if (isNullOrEmpty(request.getWorkflowName()) || isNullOrEmpty(request.getExecutionId())) {
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

        if (isNullOrEmpty(request.getWorkflowName()) || isNullOrEmpty(request.getExecutionId())) {
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

                safeDeleteTempStorageBucket(bucket[0]);
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
    }

    @Nullable
    private LzyStorageApi.CreateS3BucketResponse createTempStorageBucket(String userId, String bucket) {
        LOG.info("Creating new temp storage bucket '{}' for user '{}'", bucket, userId);

        try {
            return storageServiceClient.createS3Bucket(
                LzyStorageApi.CreateS3BucketRequest.newBuilder()
                    .setUserId(userId)
                    .setBucket(bucket)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Can't create temp bucket '{}' for user '{}': ({}) {}",
                bucket, userId, e.getStatus(), e.getMessage(), e);
            return null;
        }
    }

    private void safeDeleteTempStorageBucket(String bucket) {
        if (isNullOrEmpty(bucket)) {
            return;
        }

        LOG.info("Deleting temp storage bucket '{}'", bucket);

        try {
            @SuppressWarnings("unused")
            var resp = storageServiceClient.deleteS3Bucket(
                LzyStorageApi.DeleteS3BucketRequest.newBuilder()
                    .setBucket(bucket)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Can't delete temp bucket '{}': ({}) {}", bucket, e.getStatus(), e.getMessage(), e);
        }
    }

    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }
}
