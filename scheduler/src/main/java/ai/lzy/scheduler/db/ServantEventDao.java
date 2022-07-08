package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.ServantEvent;

import javax.annotation.Nullable;
import java.util.List;

public interface ServantEventDao {
    void save(ServantEvent event);

    @Nullable
    ServantEvent take(String servantId);

    List<ServantEvent> list(String servantId);

    void removeAllByTypes(String servantId, ServantEvent.Type... types);
    void removeAll(String servantId);
}
