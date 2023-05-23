package ai.lzy.service;

import ai.lzy.service.dao.ExecutionDao;
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

    private final ExecutionDao execDao;

    LzyServiceValidator(ExecutionDao execDao) {
        this.execDao = execDao;
    }

    boolean validate(String userId, LWFS.GetOrCreateDefaultStorageRequest req,
                     StreamObserver<? extends MessageOrBuilder> resp)
    {
        LOG.debug("Validate GetOrCreateDefaultStorageRequest: {}", safePrinter().printToString(req));
        if (Strings.isBlank(userId)) {
            var errorMes = "Cannot get or create default storage. Blank 'user id'";
            LOG.error(errorMes);
            resp.onError(Status.INVALID_ARGUMENT.withDescription(errorMes).asRuntimeException());
            return true;
        }

        return false;
    }

    boolean validate(String userId, LWFS.StartWorkflowRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        LOG.debug("Validate StartWorkflowRequest: {}", safePrinter().printToString(req));
        if (Strings.isBlank(userId) || Strings.isBlank(req.getWorkflowName())) {
            var errorMes = "Cannot start workflow. Blank 'user id' or 'workflow name'";
            LOG.error(errorMes);
            resp.onError(Status.INVALID_ARGUMENT.withDescription(errorMes).asRuntimeException());
            return true;
        }

        return false;
    }

    boolean validate(String userId, LWFS.FinishWorkflowRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "FinishWorkflowRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot finish workflow. Blank 'user id' or 'execution id' or 'workflow name'";
        return validate(new String[] {userId, req.getExecutionId(), req.getWorkflowName()}, debugMes, errorMes, resp);
    }

    boolean validate(String userId, LWFS.AbortWorkflowRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "AbortWorkflowRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot abort workflow. Blank 'user id' or 'execution id' or 'workflow name'";
        return validate(new String[] {userId, req.getExecutionId(), req.getWorkflowName()}, debugMes, errorMes, resp);
    }

    boolean validate(String userId, LWFS.ExecuteGraphRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "ExecuteGraphRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot execute graph. Blank 'user id' or 'execution id' or 'workflow name'";
        return validate(new String[] {userId, req.getExecutionId(), req.getWorkflowName()}, debugMes, errorMes, resp);
    }

    boolean validate(String userId, LWFS.GraphStatusRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "GraphStatusRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot get graph status. Blank 'user id' or 'execution id' or 'graph id'";
        return validate(new String[] {userId, req.getExecutionId(), req.getGraphId()}, debugMes, errorMes, resp);
    }

    boolean validate(String userId, LWFS.StopGraphRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "StopGraphRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot stop graph. Blank 'user id' or 'execution id' or 'graph id'";
        return validate(new String[] {userId, req.getExecutionId(), req.getGraphId()}, debugMes, errorMes, resp);
    }

    boolean validate(String userId, LWFS.ReadStdSlotsRequest req, StreamObserver<? extends MessageOrBuilder> resp) {
        var debugMes = "ReadStdSlotsRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot open stream on std slots. Blank 'user id' or 'execution id'";
        return validate(new String[] {userId, req.getExecutionId()}, debugMes, errorMes, resp);
    }

    boolean validate(String userId, LWFS.GetAvailablePoolsRequest req,
                     StreamObserver<? extends MessageOrBuilder> resp)
    {
        var debugMes = "GetAvailablePoolsRequest: " + safePrinter().printToString(req);
        var errorMes = "Cannot get available VM pools. Blank 'user id' or 'execution id'";
        return validate(new String[] {userId, req.getExecutionId()}, debugMes, errorMes, resp);
    }

    boolean validate(String[] params, String debugMes, String errorMes,
                     StreamObserver<? extends MessageOrBuilder> resp)
    {
        LOG.debug("Validate {}", debugMes);
        if (Arrays.stream(params).anyMatch(Strings::isBlank)) {
            LOG.error(errorMes);
            resp.onError(Status.INVALID_ARGUMENT.withDescription(errorMes).asRuntimeException());
            return true;
        }

        return checkPermissionOnExecution(params[0], params[1], resp);
    }

    boolean checkPermissionOnExecution(String userId, String execId, StreamObserver<? extends MessageOrBuilder> resp) {
        try {
            if (withRetries(LOG, () -> execDao.exists(userId, execId))) {
                return false;
            }

            LOG.error("Cannot find execution of user: { execId: {}, userId: {} }", execId, userId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("Cannot find execution '%s' of user '%s'"
                .formatted(execId, userId)).asRuntimeException());
        } catch (Exception e) {
            LOG.error("Cannot check that execution of user exists: { executionId: {}, userId: " +
                "{}, error: {} } ", execId, userId, e.getMessage());
            resp.onError(Status.INTERNAL.withDescription("Error while checking that user " +
                "has permissions on requested execution").asRuntimeException());
        }

        return true;
    }
}
