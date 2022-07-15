package ai.lzy.kharon.workflow;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.kharon.workflow.storage.WorkflowDataSource;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.db.Transaction;
import ai.lzy.priv.v2.LzyWorkflowApi.*;
import ai.lzy.priv.v2.LzyWorkflowGrpc.LzyWorkflowImplBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

@Singleton
public class WorkflowService extends LzyWorkflowImplBase {
    private static final Logger LOG = LogManager.getLogger(WorkflowService.class);

    private final WorkflowDataSource storage;

    @Inject
    public WorkflowService(WorkflowDataSource storage) {
        this.storage = storage;
    }

    @Override
    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[createWorkflow], uid={}, request={}.", userId, JsonUtils.printRequest(request));

        BiConsumer<io.grpc.Status, String> replyError = (rc, descr) -> {
            LOG.error("[createWorkflow], fail: rc={}, msg={}.", rc, descr);
            response.onError(rc.withDescription(descr).asException());
        };

        if (isNullOrEmpty(request.getWorkflowName())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty `workflowName`");
            return;
        }

        final String[] executionId = {null};
        final String[] error = {null};
        try {
            Transaction.execute(storage, conn -> {
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
                        error[0] = String.format(
                            "Attempt to start one more instance of workflow: active is '%s', was started at '%s'",
                            existingExecutionId, rs.getTimestamp("execution_started_at"));
                        return false;
                    }

                    executionId[0] = request.getWorkflowName() + "_" + UUID.randomUUID();
                    rs.updateString("execution_id", executionId[0]);
                    rs.updateTimestamp("execution_started_at", Timestamp.from(Instant.now()));
                    rs.updateRow();

                    LOG.info("[createWorkflow], new execution '{}' for workflow '{}' created.",
                        request.getWorkflowName(), executionId);
                } else {
                    executionId[0] = request.getWorkflowName() + "_" + UUID.randomUUID();

                    stmt = conn.prepareStatement("""
                        insert into workflows (user_id, workflow_name, created_at, execution_id, execution_started_at)
                        values (?, ?, ?, ?, ?)
                        """);
                    var now = Timestamp.from(Instant.now());
                    stmt.setString(1, userId);
                    stmt.setString(2, request.getWorkflowName());
                    stmt.setTimestamp(3, now);
                    stmt.setString(4, executionId[0]);
                    stmt.setTimestamp(5, now);
                    stmt.executeUpdate();

                    LOG.info("[createWorkflow], new workflow '{}' and execution '{}' created.",
                        request.getWorkflowName(), executionId);
                }
                return true;
            });
        } catch (Exception e) {
            LOG.error("[createWorkflow] Got SQLException: " + e.getMessage(), e);
            response.onError(e);
            return;
        }

        if (executionId[0] != null) {
            response.onNext(CreateWorkflowResponse.newBuilder()
                .setExecutionId(executionId[0])
                .setTempS3(CreateWorkflowResponse.TempS3.newBuilder()

                    .setBucket("tmp__user_" + userId + "__" + executionId[0])
                    .build())
                .build());
            response.onCompleted();
        } else {
            replyError.accept(Status.ALREADY_EXISTS, Objects.requireNonNull(error[0]));
        }
    }

    @Override
    public void attachWorkflow(AttachWorkflowRequest request, StreamObserver<AttachWorkflowResponse> response) {
        LOG.info("[finishWorkflow], request={}.", JsonUtils.printRequest(request));

        // TODO: auth
        String userId = "test";

        BiConsumer<io.grpc.Status, String> replyError = (status, descr) -> {
            LOG.error("[attachWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asException());
        };

        if (isNullOrEmpty(request.getWorkflowName()) || isNullOrEmpty(request.getExecutionId())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty 'workflowName' or 'executionId'");
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

                response.onNext(AttachWorkflowResponse.getDefaultInstance());
                response.onCompleted();
            } else {
                replyError.accept(Status.NOT_FOUND, "");
            }
        } catch (SQLException e) {
            LOG.error("[finishWorkflow] Got SQLException: " + e.getMessage(), e);
            response.onError(e);
        }
    }

    @Override
    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<FinishWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[finishWorkflow], uid={}, request={}.", userId, JsonUtils.printRequest(request));

        BiConsumer<io.grpc.Status, String> replyError = (status, descr) -> {
            LOG.error("[finishWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asException());
        };

        if (isNullOrEmpty(request.getWorkflowName()) || isNullOrEmpty(request.getExecutionId())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty 'workflowName' or 'executionId'");
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

                response.onNext(FinishWorkflowResponse.getDefaultInstance());
                response.onCompleted();
            } else {
                replyError.accept(Status.NOT_FOUND, "");
            }
        } catch (SQLException e) {
            LOG.error("[finishWorkflow] Got SQLException: " + e.getMessage(), e);
            response.onError(e);
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
    }

    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

}
