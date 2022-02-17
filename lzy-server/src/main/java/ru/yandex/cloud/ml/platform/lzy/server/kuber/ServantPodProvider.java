package ru.yandex.cloud.ml.platform.lzy.server.kuber;

import io.kubernetes.client.openapi.models.V1Pod;
import java.net.URI;
import java.util.UUID;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

public interface ServantPodProvider {
    V1Pod createServantPod(Zygote workload, String token, UUID tid, URI serverURI, String uid, String bucket)
        throws PodProviderException;
}
