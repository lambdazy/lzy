package ai.lzy.scheduler.servant;

import ai.lzy.model.graph.Env;
import ai.lzy.scheduler.models.TaskDesc;
import io.grpc.StatusRuntimeException;

public interface ServantApi {
    void configure(Env env) throws StatusRuntimeException;
    void startExecution(String taskId, TaskDesc task) throws StatusRuntimeException;
    void gracefulStop() throws StatusRuntimeException;
    void signal(int signalNumber) throws StatusRuntimeException;
}
