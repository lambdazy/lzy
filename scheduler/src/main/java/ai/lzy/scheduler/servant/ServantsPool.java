package ai.lzy.scheduler.servant;


import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;

public interface ServantsPool {
    /**
     * Waits for free servant to execute task with this provisioning. If it can, it allocates new servant.
     * Returns null if pool is stopping.
     * @param workflowId workflow in what to allocate servant
     * @param provisioning provisioning to match
     * @return allocated servant handle
     * @throws InterruptedException if interrupted while waiting
     */
    @Nullable
    Servant waitForFree(String workflowId, Provisioning provisioning) throws InterruptedException;

    void start();

    /**
     * Gracefully shutting down the pool
     */
    void shutdown();
    void waitForShutdown() throws InterruptedException;

    /**
     * Destroy pool and all servants in it
     */
    void destroy();
}
