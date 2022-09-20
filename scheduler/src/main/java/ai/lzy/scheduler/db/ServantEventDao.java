package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.ServantEvent;

import java.util.List;
import javax.annotation.Nullable;

public interface ServantEventDao {
    void save(ServantEvent event);

    @Nullable
    ServantEvent take(String servantId) throws InterruptedException;

    List<ServantEvent> list(String servantId);

    void removeAllByTypes(String servantId, ServantEvent.Type... types);
    void removeAll(String servantId);
}
