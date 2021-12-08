package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.kubernetes.client.openapi.models.V1Pod;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

public interface ProvisioningPodFactory {
    V1Pod fillPodSpecWithProvisioning(V1Pod podSpec, Zygote workload);
}
