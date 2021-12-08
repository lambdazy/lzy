package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.*;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ProvisioningPodFactoryImpl implements ProvisioningPodFactory {
    public static final V1Toleration GPU_SERVANT_POD_TOLERATION = new V1TolerationBuilder()
            .withKey("sku")
            .withOperator("Equal")
            .withValue("gpu")
            .withEffect("NoSchedule")
            .build();
    public static final List<V1Toleration> GPU_SERVANT_POD_TOLERATIONS = List.of(
            GPU_SERVANT_POD_TOLERATION
    );
    public static final V1ResourceRequirements GPU_SERVANT_POD_RESOURCE = new V1ResourceRequirementsBuilder().addToLimits("nvidia.com/gpu", Quantity.fromString("1")).build();

    @Override
    public V1Pod fillPodSpecWithProvisioning(V1Pod pod, Zygote workload) {
        final V1PodSpec podSpec = pod.getSpec();
        Objects.requireNonNull(podSpec);
        Objects.requireNonNull(pod.getMetadata());
        final String typeLabelValue;
        if (isNeedGpu(workload)) {
            typeLabelValue = "gpu";
            podSpec.setTolerations(GPU_SERVANT_POD_TOLERATIONS);
            podSpec.getContainers().get(0).setResources(GPU_SERVANT_POD_RESOURCE);
        } else {
            typeLabelValue = "cpu";
        }
        final Map<String, String> nodeSelector = Map.of("type", typeLabelValue);
        podSpec.setNodeSelector(nodeSelector);
        return null;
    }

    private static boolean isNeedGpu(Zygote workload) {
        return ((AtomicZygote) workload).provisioning().tags().anyMatch(tag -> tag.tag().contains("GPU"));
    }
}
