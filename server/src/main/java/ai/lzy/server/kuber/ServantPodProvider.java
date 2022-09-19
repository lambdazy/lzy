package ai.lzy.server.kuber;

import ai.lzy.model.graph.Provisioning;
import io.kubernetes.client.openapi.models.V1Pod;

public interface ServantPodProvider {
    V1Pod createServantLockPod(Provisioning provisioning, String servantId, String sessionId)
        throws PodProviderException;

    V1Pod createServantPod(Provisioning provisioning, String token, String servantId, String bucket, String sessionId)
        throws PodProviderException;
}
