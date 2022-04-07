package ru.yandex.cloud.ml.platform.lzy.server.kuber;

import io.kubernetes.client.openapi.models.V1Pod;
import java.net.URI;
import java.util.UUID;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;

public interface ServantPodProvider {
    V1Pod createServantPod(Provisioning provisioning, String token, String servantId, String bucket)
        throws PodProviderException;
}
