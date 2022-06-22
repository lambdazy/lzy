package ai.lzy.scheduler.servant;

import ai.lzy.scheduler.models.ServantEvent;
import java.util.List;

public interface ServantEventQueue {
    void put(ServantEvent event);
    void removeAll(String servantId, ServantEvent.Type type);
    void removeAll(String servantId);

    ServantEvent waitForNextEvent() throws InterruptedException;
}
