package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.ServantEvent;

import javax.annotation.Nullable;

public interface ServantEventDao {

    @Nullable
    ServantEvent get(String id);
    void save(ServantEvent event);

    @Nullable
    ServantEvent take(String servantId);

    @Nullable
    ServantEvent remove(String id);

    void removeAllByTypes(String servantId, ServantEvent.Type... types);
    void removeAll(String servantId);
}
