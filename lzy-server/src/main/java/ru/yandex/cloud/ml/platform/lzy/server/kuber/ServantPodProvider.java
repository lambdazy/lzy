package ru.yandex.cloud.ml.platform.lzy.server.kuber;

import io.kubernetes.client.openapi.models.V1Pod;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

import java.net.URI;
import java.util.UUID;

public interface ServantPodProvider {
    V1Pod fillPodSpecWithProvisioning(Zygote workload, String token, UUID tid, URI serverURI) throws PodProviderException;
}
