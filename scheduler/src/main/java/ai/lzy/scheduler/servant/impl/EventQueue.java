package ai.lzy.scheduler.servant.impl;

import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.models.ServantEvent;
import java.util.List;
import java.util.concurrent.DelayQueue;

public class EventQueue {
    private final ServantEventDao dao;
    private final DelayQueue<ServantEvent> queue = new DelayQueue<>();
    private final String servantId;

    public EventQueue(ServantEventDao dao, String servantId) {
        this.dao = dao;
        this.servantId = servantId;
    }

    public List<ServantEvent> waitForNextEvents() throws InterruptedException {
        List<ServantEvent> events = dao.takeBeforeNow(servantId);
        while (events.isEmpty()) {
            queue.take();
            events = dao.takeBeforeNow(servantId);  // Event can be removed from dao
        }
        return events;
    }

    public void put(ServantEvent event) {
        dao.save(event);
        queue.put(event);
    }
}
