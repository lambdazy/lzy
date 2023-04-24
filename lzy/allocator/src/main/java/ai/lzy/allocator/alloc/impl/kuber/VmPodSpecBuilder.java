package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.vmpool.VmPoolSpec;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class VmPodSpecBuilder {

    private static final Toleration GPU_VM_POD_TOLERATION = new TolerationBuilder()
        .withKey("sku")
        .withOperator("Equal")
        .withValue("gpu")
        .withEffect("NoSchedule")
        .build();

    private final PodSpecBuilder podSpecBuilder;

    private final Vm.Spec vmSpec;
    private final VmPoolSpec poolSpec;
    private final ServiceConfig config;

    public VmPodSpecBuilder(Vm.Spec vmSpec, VmPoolSpec poolSpec, KubernetesClient client, ServiceConfig config,
                            String templatePath, String podNamePrefix)
    {
        this.vmSpec = vmSpec;
        this.poolSpec = poolSpec;

        this.config = config;

        String vmId = vmSpec.vmId();
        final String podName = podNamePrefix + vmId.toLowerCase(Locale.ROOT);

        podSpecBuilder = new PodSpecBuilder(podName, templatePath, client, config);

        podSpecBuilder
            .withTolerations(List.of(GPU_VM_POD_TOLERATION))
            .withLabels(Map.of(
                KuberLabels.LZY_POD_NAME_LABEL, podName,
                KuberLabels.LZY_POD_SESSION_ID_LABEL, vmSpec.sessionId(),
                KuberLabels.LZY_VM_ID_LABEL, vmId.toLowerCase(Locale.ROOT)
            ))
            .withNodeSelector(Map.of(
                KuberLabels.NODE_POOL_LABEL, vmSpec.poolLabel(),
                KuberLabels.NODE_POOL_AZ_LABEL, vmSpec.zone(),
                KuberLabels.NODE_POOL_STATE_LABEL, "ACTIVE"
            ))
            .withEnvVars(Map.of(
                AllocatorAgent.VM_ID_KEY, vmSpec.vmId(),
                AllocatorAgent.VM_GPU_COUNT, String.valueOf(poolSpec.gpuCount())
            ));
    }

    public String getPodName() {
        return podSpecBuilder.getPodName();
    }

    public VmPodSpecBuilder withWorkloads(List<Workload> workloads, boolean init) {
        podSpecBuilder.withWorkloads(workloads, init);
        return this;
    }

    public VmPodSpecBuilder withVolumes(List<VolumeClaim> volumeClaims) {
        podSpecBuilder.withVolumes(volumeClaims);
        return this;
    }

    public VmPodSpecBuilder withHostVolumes(List<VolumeRequest> volumeRequests) {
        podSpecBuilder.withHostVolumes(volumeRequests);
        return this;
    }
    
    public VmPodSpecBuilder withEmptyDirVolume(String name, String path, EmptyDirVolumeSource emptyDir) {
        podSpecBuilder.withEmptyDirVolume(name, path, emptyDir);
        return this;
    }

    public VmPodSpecBuilder withLoggingVolume() {
        podSpecBuilder.withLoggingVolume();
        return this;
    }

    public VmPodSpecBuilder withPodAffinity(String key, String operator, String... values) {
        podSpecBuilder.withPodAffinity(key, operator, values);
        return this;
    }

    public VmPodSpecBuilder withPodAntiAffinity(String key, String operator, String... values) {
        podSpecBuilder.withPodAntiAffinity(key, operator, values);
        return this;
    }

    public Pod build() {
        return podSpecBuilder.build();
    }
}
