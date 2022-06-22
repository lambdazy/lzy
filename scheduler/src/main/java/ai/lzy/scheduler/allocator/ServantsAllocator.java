package ai.lzy.scheduler.allocator;

import java.net.URI;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;

public interface ServantsAllocator {
    void allocate(
        String workflowId,
        String servantId,
        Provisioning provisioning,
        Env env
    );

    void destroy(String workflowId, String servantId);
    void register(String servantId, URI servantUri);
}
