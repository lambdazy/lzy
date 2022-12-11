package ai.lzy.scheduler.worker.impl;

import ai.lzy.scheduler.db.WorkerEventDao;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class EventQueueManager {
    private final Map<WorkerKey, EventQueue> queueMap = new ConcurrentHashMap<>();
    private final WorkerEventDao dao;

    @Inject
    public EventQueueManager(WorkerEventDao dao) {
        this.dao = dao;
    }

    public EventQueue get(String workflowId, String workerId) {
        return queueMap.computeIfAbsent(new WorkerKey(workflowId, workerId), k -> new EventQueue(dao, workerId));
    }

    private record WorkerKey(String workflowId, String workerId) {}
}
