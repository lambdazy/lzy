package ai.lzy.service.workflow;

import ai.lzy.common.IdGenerator;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.WorkflowDao;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

public record ExecutionStepContext(String opId, String userId, String wfName, String execId,
                                   WorkflowDao wfDao, ExecutionDao execDao, String idempotencyKey,
                                   Function<StatusRuntimeException, OperationRunnerBase.StepResult> failAction,
                                   Logger log, String logPrefix, IdGenerator idGenerator) {}