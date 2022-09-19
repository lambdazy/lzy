package ai.lzy.scheduler.servant;

import ai.lzy.model.TaskDesc;
import ai.lzy.model.graph.Env;
import io.grpc.StatusRuntimeException;

public interface ServantApi {
    void configure(Env env) throws StatusRuntimeException;
    void startExecution(String taskId, TaskDesc task) throws StatusRuntimeException;
    void stop() throws StatusRuntimeException;
}
