package ai.lzy.scheduler.allocator.impl.kuber;

import ai.lzy.model.graph.Provisioning;
import io.kubernetes.client.openapi.models.V1Pod;

public interface ServantPodProvider {
    V1Pod createServantPod(Provisioning provisioning, String token, String servantId, String workflowId)
        throws PodProviderException;
}
