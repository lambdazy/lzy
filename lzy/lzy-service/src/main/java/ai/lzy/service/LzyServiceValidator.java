package ai.lzy.service;

import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;

@Singleton
final class LzyServiceValidator {
    private static final Logger LOG = LogManager.getLogger(LzyServiceValidator.class);

    private final WorkflowDao wfDao;
    private final ExecutionDao execDao;

    LzyServiceValidator(WorkflowDao wfDao, ExecutionDao execDao) {
        this.wfDao = wfDao;
        this.execDao = execDao;
    }

    boolean validate(LWFS.StartWorkflowRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        LOG.debug("Validate StartWorkflowRequest: {}", safePrinter().printToString(req));
        if (Strings.isBlank(req.getWorkflowName()) || !req.hasSnapshotStorage()) {
            var errorMes = "Cannot start workflow. Blank 'workflow name' or 'storage config'";
            LOG.error(errorMes);
            resp.onError(Status.INVALID_ARGUMENT.withDescription(errorMes).asRuntimeException());
            return true;
        }

        return false;
    }

    boolean validate(String userId, LWFS.FinishWorkflowRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "FinishWorkflowRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot finish workflow. Blank 'execution id' or 'workflow name' or 'reason'";
        return validate(userId, new String[] {req.getExecutionId(), req.getWorkflowName(), req.getReason()}, debugMes,
            errorMes, resp);
    }

    boolean validate(String userId, LWFS.AbortWorkflowRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "AbortWorkflowRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot abort workflow. Blank 'execution id' or 'workflow name' or 'reason'";
        return validate(userId, new String[] {req.getExecutionId(), req.getWorkflowName(), req.getReason()}, debugMes,
            errorMes, resp);
    }

    boolean validate(String userId, LWFS.ExecuteGraphRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "ExecuteGraphRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot execute graph. Blank 'execution id' or 'workflow name' or 'graph'";
        return validate(userId, new String[] {req.getExecutionId(), req.getWorkflowName(), req.hasGraph() ? "ok" : ""},
            debugMes, errorMes, resp);
    }

    boolean validate(String userId, LWFS.GraphStatusRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "GraphStatusRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot get graph status. Blank 'execution id' or 'workflow name' or 'graph id'";
        return validate(userId, new String[] {req.getExecutionId(), req.getWorkflowName(), req.getGraphId()}, debugMes,
            errorMes, resp);
    }

    boolean validate(String userId, LWFS.StopGraphRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "StopGraphRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot stop graph. Blank 'execution id' or 'graph id'";
        return validate(userId, new String[] {req.getExecutionId(), req.getWorkflowName(), req.getGraphId()}, debugMes,
            errorMes, resp);
    }

    boolean validate(String userId, LWFS.ReadStdSlotsRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "ReadStdSlotsRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot open stream on std slots. Blank 'execution id' or 'workflow name'";
        return validate(userId, new String[] {req.getExecutionId(), req.getWorkflowName()}, debugMes, errorMes, resp);
    }

    boolean validate(String userId, LWFS.GetAvailablePoolsRequest req,
                     StreamObserver<? extends MessageOrBuilder> resp)
    {
        var debugMes = "GetAvailablePoolsRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot get available VM pools. Blank 'execution id'";
        return validate(userId, new String[] {req.getExecutionId(), req.getWorkflowName()}, debugMes, errorMes, resp);
    }

    boolean validate(String userId, String[] params, String debugMes, String errorMes,
                     StreamObserver<? extends MessageOrBuilder> resp)
    {
        LOG.debug("Validate {}", debugMes);
        if (Arrays.stream(params).anyMatch(Strings::isBlank)) {
            LOG.error(errorMes);
            resp.onError(Status.INVALID_ARGUMENT.withDescription(errorMes).asRuntimeException());
            return true;
        }

        return checkExecution(userId, params[0], params[1], resp);
    }

    boolean checkExecution(String userId, String execId, String wfName,
                           StreamObserver<? extends MessageOrBuilder> resp)
    {
        try {
            if (withRetries(LOG, () -> wfDao.exists(userId, wfName))) {
                if (withRetries(LOG, () -> execDao.exists(userId, execId))) {
                    return false;
                }

                LOG.error("Cannot find execution of user: { userId: {}, execId: {} }", userId, execId);
                resp.onError(Status.INVALID_ARGUMENT.withDescription("Cannot find execution '%s' of user '%s'"
                    .formatted(execId, userId)).asRuntimeException());
                return true;
            }

            LOG.error("Cannot find workflow of user: { userId: {}, wfName: {} }", userId, wfName);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("Cannot find workflow '%s' of user '%s'"
                .formatted(wfName, userId)).asRuntimeException());
        } catch (NotFoundException nfe) {
            LOG.error("Cannot find workflow of user: { userId: {}, wfName: {} }", userId, wfName);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("Cannot find workflow '%s' of user '%s'"
                .formatted(wfName, userId)).asRuntimeException());
        } catch (Exception e) {
            LOG.error("Cannot check that workflow with active execution exists: { wfName: {}, execId: {}, userId: " +
                "{}, error: {} } ", wfName, execId, userId, e.getMessage());
            resp.onError(Status.INTERNAL.withDescription("Error on validation workflow execution")
                .asRuntimeException());
        }

        return true;
    }
}
