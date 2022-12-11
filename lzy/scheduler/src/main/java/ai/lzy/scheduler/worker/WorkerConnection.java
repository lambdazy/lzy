package ai.lzy.scheduler.worker;

public interface WorkerConnection {
    WorkerApi api();
    void close();
}
