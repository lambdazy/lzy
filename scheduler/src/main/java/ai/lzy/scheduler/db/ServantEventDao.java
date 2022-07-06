package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.ServantEvent;

import javax.annotation.Nullable;

public interface ServantEventDao {
    void save(ServantEvent event);

    @Nullable
    ServantEvent take(String servantId);

    void removeAllByTypes(String servantId, ServantEvent.Type... types);
    void removeAll(String servantId);
}
