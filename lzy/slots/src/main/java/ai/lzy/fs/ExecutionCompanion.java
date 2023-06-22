package ai.lzy.fs;

interface ExecutionCompanion {
    /**
     * Prepare companion for execution
     */
    void beforeExecution() throws Exception;

    /**
     * Finalize companion after execution
     */
    void afterExecution() throws Exception;

    /**
     * Close companion. It must clean up all resources and fail all pending operations
     */
    void close();
}
