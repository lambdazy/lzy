package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.WorkerEvent;

import java.util.List;
import javax.annotation.Nullable;

public interface WorkerEventDao {
    void save(WorkerEvent event);

    @Nullable
    WorkerEvent take(String workerId) throws InterruptedException;

    @Nullable
    WorkerEvent takeById(String eventId);

    List<WorkerEvent> list(String workerId);

    void removeAllByTypes(String workerId, WorkerEvent.Type... types);
    void removeAll(String workerId);
}
