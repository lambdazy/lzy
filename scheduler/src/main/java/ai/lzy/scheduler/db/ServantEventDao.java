package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.ServantEvent;
import java.util.List;
import javax.annotation.Nullable;

public interface ServantEventDao {

    @Nullable
    ServantEvent get(String id);
    void save(ServantEvent event);

    List<ServantEvent> takeBeforeNow(String servantId);

    @Nullable
    ServantEvent remove(String id);

    void removeAll(String servantId, ServantEvent.Type... types);
    void removeAll(String servantId);
}
