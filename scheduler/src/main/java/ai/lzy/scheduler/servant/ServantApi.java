package ai.lzy.scheduler.servant;

import ai.lzy.scheduler.models.TaskDesc;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;

public interface ServantApi {
    void configure(Env env) throws StatusRuntimeException;
    void startExecution(String taskId, TaskDesc task) throws StatusRuntimeException;
    void gracefulStop() throws StatusRuntimeException;
    void signal(int signalNumber) throws StatusRuntimeException;

    record Result(Integer rc, String description) {}
}
