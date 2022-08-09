package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Yaml;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Singleton
@Requires(property = "allocator.kuber-allocator.enabled", value = "true")
public class VmPodProvider {
    private static final Logger LOG = LogManager.getLogger(VmPodProvider.class);
    private static final List<V1Toleration> GPU_VM_POD_TOLERATIONS = List.of(
        new V1Toleration()
            .key("sku")
            .operator("Equal")
            .value("gpu")
            .effect("NoSchedule"));

    public static final String NODE_POOL_KEY = "lzy.ai/node-pool";
    public static final String LZY_POD_NAME_LABEL = "lzy.ai/pod-name";

    private final ServiceConfig config;

    @Inject
    public VmPodProvider(ServiceConfig config) {
        this.config = config;
    }

    public V1Pod createVmPod(Vm vm) {
        try {
            final ApiClient client = ClientBuilder.cluster().build();
            Configuration.setDefaultApiClient(client);
        } catch (IOException e) {
            LOG.error("IO error while finding Kubernetes config");
            throw new RuntimeException("cannot load kuber api client", e);
        }

        final V1Pod pod;
        final File file = new File(config.kuberAllocator().podTemplatePath());
        try {
            pod = (V1Pod) Yaml.load(file);
        } catch (IOException e) {
            LOG.error("IO error while loading yaml file {}", file.getPath());
            throw new RuntimeException("cannot load vm yaml file", e);
        }

        Objects.requireNonNull(pod.getSpec());
        Objects.requireNonNull(pod.getMetadata());

        for (var workload: vm.workloads()) {
            final var container = new V1Container();
            workload.env().forEach((key, value) -> container.addEnvItem(new V1EnvVar().name(key).value(value)));
            container.setArgs(workload.args());
            container.setName(workload.name());
            container.setImage(workload.image());
            container.setPorts(
                workload.portBindings().entrySet().stream()
                    .map(e -> {
                        var spec = new V1ContainerPort();
                        spec.setContainerPort(e.getKey());
                        spec.setHostPort(e.getValue());
                        return spec;
                    })
                    .toList()
            );
            final var context = new V1SecurityContext();
            context.setPrivileged(true);
            context.setRunAsUser(0L);
            container.setSecurityContext(context);
            pod.getSpec().addContainersItem(container);
        }

        final String podName = "lzy-vm-" + vm.vmId().toLowerCase(Locale.ROOT);
        // k8s pod name can only contain symbols [-a-z0-9]
        pod.getMetadata().setName(podName.replaceAll("[^-a-z0-9]", "-"));
        var labels = pod.getMetadata().getLabels();
        Objects.requireNonNull(labels);
        labels.put(LZY_POD_NAME_LABEL, podName);
        pod.getMetadata().setLabels(labels);
        pod.getSpec().setTolerations(GPU_VM_POD_TOLERATIONS);
        final Map<String, String> nodeSelector = Map.of(NODE_POOL_KEY, vm.poolId());
        pod.getSpec().setNodeSelector(nodeSelector);
        return pod;
    }
}
