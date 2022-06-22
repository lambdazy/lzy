package ai.lzy.scheduler.servant;

import ai.lzy.scheduler.models.TaskDesc;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;

public interface ServantApi {
    void configure(Env env);
    void startExecution(TaskDesc task);
    void gracefulStop();
    void signal(int signalNumber);
}
