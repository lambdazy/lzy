package ai.lzy.scheduler.servant;

import java.net.URL;
import javax.annotation.Nullable;

public interface ServantConnectionManager {

    @Nullable
    ServantConnection get(String workflowId, String servantId);

    interface ServantConnection extends AutoCloseable {
        ServantApi api();
        void close();
    }

    void register(String workflowId, String servantId, URL servantUrl);
}
