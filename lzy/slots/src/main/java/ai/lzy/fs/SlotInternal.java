package ai.lzy.fs;

import java.util.concurrent.CompletableFuture;

interface SlotInternal {
    /**
     * Prepare companion for execution
     */
    CompletableFuture<Void> beforeExecution();

    /**
     * Finalize companion after execution
     */
    CompletableFuture<Void> afterExecution();

    /**
     * Close companion. It must clean up all resources and fail all pending operations
     */
    void close();
}
