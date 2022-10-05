package ai.lzy.scheduler.servant.impl;

import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.models.ServantEvent;

import java.util.concurrent.DelayQueue;

public class EventQueue {
    private final ServantEventDao dao;
    private final DelayQueue<ServantEvent> queue = new DelayQueue<>();
    private final String servantId;

    public EventQueue(ServantEventDao dao, String servantId) {
        this.dao = dao;
        this.servantId = servantId;
        restore();
    }

    public ServantEvent waitForNext() throws InterruptedException {
        ServantEvent event = dao.take(servantId);
        while (event == null) {
            queue.take();
            event = dao.take(servantId);  // Event can be removed from dao
        }
        return event;
    }

    public void put(ServantEvent event) {
        dao.save(event);
        queue.put(event);
    }

    private void restore() {
        final var events = dao.list(servantId);
        queue.addAll(events);
    }
}
