package ru.yandex.cloud.ml.platform.lzy.gateway.workflow;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.gateway.workflow.configs.WorkflowServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.gateway.workflow.storage.Storage;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.*;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGrpc.LzyApiImplBase;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.function.BiConsumer;

@Singleton
public class WorkflowService extends LzyApiImplBase {
    public static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final WorkflowServiceConfig config;
    private final Storage storage;
    private final ManagedChannel serverChannel;
    private final LzyServerGrpc.LzyServerBlockingStub serverClient;

    @Inject
    public WorkflowService(WorkflowServiceConfig config, Storage storage) {
        this.config = config;
        this.storage = storage;

        this.serverChannel = ChannelBuilder
            .forAddress(this.config.getServerAddress())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        this.serverClient = LzyServerGrpc.newBlockingStub(serverChannel);
    }

    @Override
    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> response) {
        LOG.info("[createWorkflow], request={}.", JsonUtils.printRequest(request));

        // TODO: auth
        String userId = "test";

        BiConsumer<CreateWorkflowResponse.ErrorCode, String> replyError = (rc, descr) -> {
            LOG.error("[createWorkflow], fail: rc={}, msg={}.", rc, descr);
            response.onNext(CreateWorkflowResponse.newBuilder()
                .setError(CreateWorkflowResponse.Error.newBuilder()
                    .setCode(rc.getNumber())
                    .setDescription(descr)
                    .build())
                .build());
            response.onCompleted();
        };

        if (isNullOrEmpty(request.getWorkflowName())) {
            replyError.accept(CreateWorkflowResponse.ErrorCode.BAD_REQUEST, "Empty `workflowName`");
            return;
        }

        String executionId;

        try (var conn = storage.connect()) {
            conn.setAutoCommit(false);

            var stmt = conn.prepareStatement("""
                select user_id, workflow_name, execution_id, execution_started_at
                from workflows
                where user_id=? and workflow_name=?
                for update
                """, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            stmt.setString(1, userId);
            stmt.setString(2, request.getWorkflowName());

            var rs = stmt.executeQuery();
            if (rs.next()) {
                var existingExecutionId = rs.getString("execution_id");
                if (!isNullOrEmpty(existingExecutionId)) {
                    replyError.accept(CreateWorkflowResponse.ErrorCode.ALREADY_EXISTS, String.format(
                        "Attempt to start one more instance of workflow: active is '%s', was started at '%s'",
                        existingExecutionId, rs.getTimestamp("execution_started_at")));
                    conn.rollback();
                    conn.setAutoCommit(true);
                    return;
                }

                executionId = request.getWorkflowName() + "_" + UUID.randomUUID();
                rs.updateString("execution_id", executionId);
                rs.updateTimestamp("execution_started_at", Timestamp.from(Instant.now()));
                rs.updateRow();

                conn.commit();
                LOG.info("[createWorkflow], new execution '{}' for workflow '{}' created.",
                    request.getWorkflowName(), executionId);
            } else {
                executionId = request.getWorkflowName() + "_" + UUID.randomUUID();

                stmt = conn.prepareStatement("""
                    insert into workflows (user_id, workflow_name, created_at, execution_id, execution_started_at)
                    values (?, ?, ?, ?, ?)
                    """);
                var now = Timestamp.from(Instant.now());
                stmt.setString(1, userId);
                stmt.setString(2, request.getWorkflowName());
                stmt.setTimestamp(3, now);
                stmt.setString(4, executionId);
                stmt.setTimestamp(5, now);
                stmt.executeUpdate();

                conn.commit();
                LOG.info("[createWorkflow], new workflow '{}' and execution '{}' created.",
                    request.getWorkflowName(), executionId);
            }

            conn.setAutoCommit(true);

            if (startPortal(request.getWorkflowName(), executionId)) {
                response.onNext(CreateWorkflowResponse.newBuilder()
                        .setSuccess(CreateWorkflowResponse.Success.newBuilder()
                                .setExecutionId(executionId)
                                .build())
                        .build());
                response.onCompleted();
            } else {
                // rollback and return error
                stmt = conn.prepareStatement("""
                    update workflows
                    set execution_id = null, execution_started_at = null
                    where user_id=? and workflow_name=?
                    """);
                stmt.setString(1, userId);
                stmt.setString(2, request.getWorkflowName());
                stmt.executeUpdate();

                replyError.accept(CreateWorkflowResponse.ErrorCode.ERROR, "Error while creating portal");
            }
        } catch (SQLException e) {
            LOG.error("[createWorkflow] Got SQLException: " + e.getMessage(), e);
            response.onError(e);
        }
    }

    @Override
    public void attachWorkflow(AttachWorkflowRequest request, StreamObserver<AttachWorkflowResponse> response) {
        LOG.info("[finishWorkflow], request={}.", JsonUtils.printRequest(request));

        // TODO: auth
        String userId = "test";

        BiConsumer<AttachWorkflowResponse.Status, String> replyError = (status, descr) -> {
            LOG.error("[attachWorkflow], fail: status={}, msg={}.", status, descr);
            response.onNext(AttachWorkflowResponse.newBuilder()
                .setStatus(status.getNumber())
                .setDescription(descr)
                .build());
            response.onCompleted();
        };

        if (isNullOrEmpty(request.getWorkflowName()) || isNullOrEmpty(request.getExecutionId())) {
            replyError.accept(AttachWorkflowResponse.Status.BAD_REQUEST, "Empty 'workflowName' or 'executionId'");
            return;
        }

        try (var conn = storage.connect()) {
            var stmt = conn.prepareStatement("""
                select count(*)
                from workflows
                where user_id=? and workflow_name=? and execution_id=?
                """, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            stmt.setString(1, userId);
            stmt.setString(2, request.getWorkflowName());
            stmt.setString(2, request.getExecutionId());

            var rs = stmt.executeQuery();

            if (rs.next()) {
                LOG.info("[attachWorkflow] workflow '{}/{}' successfully attached.",
                    request.getWorkflowName(), request.getExecutionId());

                response.onNext(AttachWorkflowResponse.newBuilder()
                    .setStatus(FinishWorkflowResponse.Status.SUCCESS.getNumber())
                    .setDescription("")
                    .build());
                response.onCompleted();
            } else {
                replyError.accept(AttachWorkflowResponse.Status.NOT_FOUND, "");
            }
        } catch (SQLException e) {
            LOG.error("[finishWorkflow] Got SQLException: " + e.getMessage(), e);
            response.onError(e);
        }
    }

    @Override
    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<FinishWorkflowResponse> response) {
        LOG.info("[finishWorkflow], request={}.", JsonUtils.printRequest(request));

        // TODO: auth
        String userId = "test";

        BiConsumer<FinishWorkflowResponse.Status, String> replyError = (status, descr) -> {
            LOG.error("[finishWorkflow], fail: status={}, msg={}.", status, descr);
            response.onNext(FinishWorkflowResponse.newBuilder()
                .setStatus(status.getNumber())
                .setDescription(descr)
                .build());
            response.onCompleted();
        };

        if (isNullOrEmpty(request.getWorkflowName()) || isNullOrEmpty(request.getExecutionId())) {
            replyError.accept(FinishWorkflowResponse.Status.BAD_REQUEST, "Empty 'workflowName' or 'executionId'");
            return;
        }

        try (var conn = storage.connect()) {
            var stmt = conn.prepareStatement("""
                update workflows
                set execution_id = null, execution_started_at = null
                where user_id=? and workflow_name=? and execution_id=?
                """);
            stmt.setString(1, userId);
            stmt.setString(2, request.getWorkflowName());
            stmt.setString(3, request.getExecutionId());

            int updated = stmt.executeUpdate();
            // var rs = stmt.getGeneratedKeys();
            if (updated != 0) {
                // TODO: start finish workflow process (cleanup all services)

                response.onNext(FinishWorkflowResponse.newBuilder()
                    .setStatus(FinishWorkflowResponse.Status.SUCCESS.getNumber())
                    .setDescription("")
                    .build());
                response.onCompleted();
            } else {
                replyError.accept(FinishWorkflowResponse.Status.NOT_FOUND, "");
            }
        } catch (SQLException e) {
            LOG.error("[finishWorkflow] Got SQLException: " + e.getMessage(), e);
            response.onError(e);
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        serverChannel.shutdown();
    }

    private boolean startPortal(String workflowName, String executionId) {
        LOG.info("[createWorkflow], launch portal for workflow '{}/{}'...", workflowName, executionId);

        /// oh f*ck... we don't want to deal with a stream here...
        /*
        var progress = serverClient.start(Tasks.TaskSpec.newBuilder()
            .setAuth()
            .setTid("1")
            .setZygote()
            .addAssignments()
            .build());
        */
        return true;
    }

    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

}
