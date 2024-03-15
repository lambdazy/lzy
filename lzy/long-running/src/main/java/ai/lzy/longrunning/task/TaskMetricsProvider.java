package ai.lzy.longrunning.task;

import io.prometheus.client.Gauge;

public interface TaskMetricsProvider {
    Gauge schedulerErrors();
    Gauge schedulerResolveErrors(OperationTaskResolver.Status status);
    Gauge queueSize();
    Gauge runningTasks();
}
