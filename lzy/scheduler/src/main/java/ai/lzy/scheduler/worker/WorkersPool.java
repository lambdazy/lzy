package ai.lzy.scheduler.worker;

import ai.lzy.model.operation.Operation;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

public interface WorkersPool {

    /**
     * Waits for free worker to execute task with this requirements. If it can, it allocates new worker.
     * Returns null if pool is stopping.
     * @param userId workflow belongs to
     * @param workflowName workflow in what to allocate worker
     * @param provisioning requirements to match
     * @return allocated worker future.
     */
    @Nullable
    CompletableFuture<Worker> waitForFree(String userId, String workflowName, Operation.Requirements provisioning);

    /**
     * Gracefully shutting down the pool
     */
    void shutdown();
    void waitForShutdown() throws InterruptedException;
}
