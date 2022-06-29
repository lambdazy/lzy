package ai.lzy.scheduler.allocator;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import io.grpc.StatusException;

import java.net.URI;

public interface ServantsAllocator {

    /**
     * Request servant allocation
     * @return Metadata to be saved in servant state
     */
    AllocateResult allocate(String workflowId, String servantId, Provisioning provisioning, Env env);

    void destroy(String workflowId, String servantId) throws Exception;
    void register(String workflowId, String servantId, URI servantUri, String servantToken) throws StatusException;

    record AllocateResult(String allocationToken, String allocationMeta) {}
}
