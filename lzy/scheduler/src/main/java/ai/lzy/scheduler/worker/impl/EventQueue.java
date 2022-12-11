package ai.lzy.scheduler.worker.impl;

import ai.lzy.scheduler.db.WorkerEventDao;
import ai.lzy.scheduler.models.WorkerEvent;

import java.util.concurrent.DelayQueue;

public class EventQueue {
    private final WorkerEventDao dao;
    private final DelayQueue<WorkerEvent> queue = new DelayQueue<>();
    private final String workerId;

    public EventQueue(WorkerEventDao dao, String workerId) {
        this.dao = dao;
        this.workerId = workerId;
        restore();
    }

    public WorkerEvent waitForNext() throws InterruptedException {
        WorkerEvent event = dao.take(workerId);
        while (event == null) {
            queue.take();
            event = dao.take(workerId);  // Event can be removed from dao
        }
        return event;
    }

    public void put(WorkerEvent event) {
        dao.save(event);
        queue.put(event);
    }

    private void restore() {
        final var events = dao.list(workerId);
        queue.addAll(events);
    }
}
