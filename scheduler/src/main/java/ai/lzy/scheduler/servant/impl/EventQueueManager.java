package ai.lzy.scheduler.servant.impl;

import ai.lzy.scheduler.db.ServantEventDao;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class EventQueueManager {
    private final Map<ServantKey, EventQueue> queueMap = new ConcurrentHashMap<>();
    private final ServantEventDao dao;

    @Inject
    public EventQueueManager(ServantEventDao dao) {
        this.dao = dao;
    }

    public EventQueue get(String workflowId, String servantId) {
        return queueMap.computeIfAbsent(new ServantKey(workflowId, servantId), k -> new EventQueue(dao, servantId));
    }

    private record ServantKey(String workflowId, String servantId) {}
}
