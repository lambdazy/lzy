package ai.lzy.scheduler.servant;

import ai.lzy.model.operation.Operation;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

public interface ServantsPool {

    /**
     * Waits for free servant to execute task with this requirements. If it can, it allocates new servant.
     * Returns null if pool is stopping.
     * @param workflowName workflow in what to allocate servant
     * @param provisioning requirements to match
     * @return allocated servant future.
     */
    @Nullable
    CompletableFuture<Servant> waitForFree(String workflowName, Operation.Requirements provisioning);

    /**
     * Gracefully shutting down the pool
     */
    void shutdown();
    void waitForShutdown() throws InterruptedException;
}
