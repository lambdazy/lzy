package ai.lzy.service.operations;

import ai.lzy.common.IdGenerator;
import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.db.Storage;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.GraphDao;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

public record ExecutionStepContext(String opId, String userId, String wfName, String execId, Storage storage,
                                   WorkflowDao wfDao, ExecutionDao execDao, GraphDao graphDao,
                                   RenewableJwt internalUserCredentials, String idempotencyKey,
                                   Function<StatusRuntimeException, StepResult> failAction,
                                   Logger log, String logPrefix, IdGenerator idGenerator) {}