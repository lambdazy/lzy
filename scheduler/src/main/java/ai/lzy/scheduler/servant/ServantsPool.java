package ai.lzy.scheduler.servant;


import ai.lzy.model.graph.Provisioning;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

public interface ServantsPool {

    /**
     * Waits for free servant to execute task with this provisioning. If it can, it allocates new servant.
     * Returns null if pool is stopping.
     * @param workflowName workflow in what to allocate servant
     * @param provisioning provisioning to match
     * @return allocated servant future.
     */
    @Nullable
    CompletableFuture<Servant> waitForFree(String workflowName, Provisioning provisioning);

    /**
     * Gracefully shutting down the pool
     */
    void shutdown();
    void waitForShutdown() throws InterruptedException;
}
