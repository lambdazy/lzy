package ai.lzy.scheduler.allocator;

import ai.lzy.model.graph.Provisioning;
import io.grpc.StatusException;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

public interface ServantsAllocator {

    void allocate(String workflowId, String servantId, Provisioning provisioning);

    void destroy(String workflowId, String servantId) throws Exception;

    record AllocateResult(String allocationToken, String allocationMeta) {}
}
