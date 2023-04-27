package ai.lzy.allocator.alloc.impl.kuber;

import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
@Secondary
public class DummyNodeRemover implements NodeRemover {
    private static final Logger LOG = LogManager.getLogger(DummyNodeRemover.class);

    @Override
    public void removeNode(String vmId, String nodeName, String nodeInstanceId) {
        LOG.info("Remove node {} (instance {}, vm {})", nodeName, nodeInstanceId, vmId);
    }
}
