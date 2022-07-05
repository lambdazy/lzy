package ai.lzy.scheduler.allocator;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import io.grpc.StatusException;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

import java.net.URI;

public interface ServantsAllocator {

    void allocate(String workflowId, String servantId, Provisioning provisioning);

    void destroy(String workflowId, String servantId) throws Exception;
    void register(String workflowId, String servantId, HostAndPort servantUri, String servantToken) throws StatusException;

    record AllocateResult(String allocationToken, String allocationMeta) {}
}
