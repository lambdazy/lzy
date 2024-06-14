package ai.lzy.longrunning.task;

import io.prometheus.client.Gauge;

public class StubMetricsProvider implements TaskMetricsProvider {
    
    @Override
    public Gauge schedulerErrors() {
        return Gauge.build()
            .name("scheduler_errors")
            .help("help")
            .create();
    }

    @Override
    public Gauge schedulerResolveErrors(OperationTaskResolver.Status status) {
        return Gauge.build()
            .name("scheduler_resolve_errors")
            .help("help")
            .create();
    }

    @Override
    public Gauge queueSize() {
        return Gauge.build()
            .name("queue_size")
            .help("help")
            .create();
    }

    @Override
    public Gauge runningTasks() {
        return Gauge.build()
            .name("running_tasks")
            .help("help")
            .create();
    }
}
