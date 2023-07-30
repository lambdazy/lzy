package ai.lzy.longrunning.task;

import io.prometheus.client.Gauge;

public interface TaskMetricsProvider {
    Gauge schedulerErrors(String instanceId);
    Gauge schedulerResolveErrors(String instanceId, OperationTaskResolver.Status status);
    Gauge queueSize(String instanceId);
}
