package ai.lzy.longrunning.task;

import ai.lzy.longrunning.OperationsExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TaskExecutor {

    private static final Logger LOG = LogManager.getLogger(TaskExecutor.class);

    private final TaskQueue taskQueue;
    private final OperationsExecutor operationsExecutor;
    private final TaskResolver resolver;
    private final ScheduledExecutorService scheduler;
    private final Duration initialDelay;
    private final Duration executionDelay;

    private volatile boolean started = false;
    private volatile boolean disabled = false;

    public TaskExecutor(TaskQueue taskQueue, OperationsExecutor operationsExecutor, TaskResolver resolver,
                        Duration initialDelay, Duration executionDelay)
    {
        this.taskQueue = taskQueue;
        this.operationsExecutor = operationsExecutor;
        this.resolver = resolver;
        this.initialDelay = initialDelay;
        this.executionDelay = executionDelay;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

    }

    //todo support worker failures - retry locked tasks after restart
    public void start() {
        if (started) {
            throw new IllegalStateException("Task executor has already started!");
        }
        started = true;
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                for (Task task : taskQueue.pollRemaining()) {
                    if (disabled) {
                        return;
                    }
                    var resolvedAction = resolver.resolve(task);
                    operationsExecutor.startNew(resolvedAction);
                }
            } catch (Exception e) {
                LOG.error("Got exception while scheduling task", e);
            }
        }, initialDelay.toMillis(), executionDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        disabled = true;
        scheduler.shutdown();
    }
}
