package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.TransactionHandle;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

@Singleton
@Requires(property = "allocator.kuber-allocator.enabled", value = "true")
public class KuberVmAllocator implements VmAllocator {
    private static final Logger LOG = LogManager.getLogger(KuberVmAllocator.class);
    private static final String NAMESPACE = "default";
    private static final List<V1Toleration> GPU_VM_POD_TOLERATIONS = List.of(
        new V1Toleration()
            .key("sku")
            .operator("Equal")
            .value("gpu")
            .effect("NoSchedule"));
    private static final String NAMESPACE_KEY = "namespace";
    private static final String POD_NAME_KEY = "pod-name";

    private final CoreV1Api api;
    private final ServiceConfig config;
    private final VmDao dao;

    @Inject
    public KuberVmAllocator(ServiceConfig config, VmDao dao) {
        this.config = config;
        this.dao = dao;
        try {
            Configuration.setDefaultApiClient(Config.defaultClient());
            api = new CoreV1Api();
        } catch (IOException e) {
            throw new RuntimeException("Cannot init KuberVmAllocator: ", e);
        }
    }

    @Override
    public void allocate(Vm vm, @Nullable TransactionHandle transaction) {
        final V1Pod vmPodSpec = createVmPodSpec(vm);
        final V1Pod pod;
        try {
            pod = api.createNamespacedPod(NAMESPACE, vmPodSpec, null, null, null, null);
        } catch (ApiException e) {
            LOG.error("Error while creating pod", e);
            throw new RuntimeException("Exception while creating pod in kuber", e);
        }
        LOG.debug("Created pod in Kuber: {}", pod);
        Objects.requireNonNull(pod.getMetadata());
        Objects.requireNonNull(pod.getMetadata().getNamespace());
        Objects.requireNonNull(pod.getMetadata().getName());
        dao.saveAllocatorMeta(vm.vmId(), Map.of(
            NAMESPACE_KEY, pod.getMetadata().getNamespace(),
            POD_NAME_KEY, pod.getMetadata().getName()
        ), transaction);
    }

    private void terminate(String namespace, String name) {
        try {
            if (getPod(namespace, name) != null) {
                api.deleteNamespacedPod(name, namespace, null, null, 0, null, null, null);
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    private V1Pod getPod(String namespace, String name) throws ApiException {
        final V1PodList listNamespacedPod = api.listNamespacedPod(
            namespace,
            null, null, null, null,
            KuberLabels.LZY_POD_NAME_LABEL + "=" + name,
            1,
            null, null, null,
            Boolean.FALSE
        );
        if (listNamespacedPod.getItems().size() < 1) {
            return null;
        }
        final var podSpec = listNamespacedPod.getItems().get(0);
        if (podSpec.getMetadata() != null
            && podSpec.getMetadata().getName() != null
            && podSpec.getMetadata().getName().equals(name)) {
            return podSpec;
        }
        return null;
    }

    @Override
    public void deallocate(Vm vm) {
        final var meta = dao.getAllocatorMeta(vm.vmId(), null);
        if (meta == null) {
            throw new RuntimeException("Cannot get allocatorMeta");
        }
        terminate(meta.get(NAMESPACE_KEY), meta.get(POD_NAME_KEY));
    }

    @Override
    public boolean validateRunning(Vm vm) {
        final var meta = dao.getAllocatorMeta(vm.vmId(), null);
        if (meta == null) {
            LOG.error("Metadata not found");
            return false;
        }
        final V1Pod pod;
        try {
            pod = getPod(meta.get(NAMESPACE_KEY), meta.get(POD_NAME_KEY));
        } catch (ApiException e) {
            LOG.error("Error while getting pod while validating", e);
            return false;
        }
        if (pod == null) {
            LOG.error("Pod not found while validating");
            return false;
        }

        return pod.getStatus() != null
            && pod.getStatus().getPhase() != null
            && pod.getStatus().getPhase().equals("RUNNING");
    }

    public V1Pod createVmPodSpec(Vm vm) {
        final V1Pod pod = readPod();
        Objects.requireNonNull(pod.getSpec());
        Objects.requireNonNull(pod.getMetadata());

        buildContainers(vm, pod);

        final String podName = "lzy-vm-" + vm.vmId().toLowerCase(Locale.ROOT);
        // k8s pod name can only contain symbols [-a-z0-9]
        pod.getMetadata().setName(podName.replaceAll("[^-a-z0-9]", "-"));
        var labels = pod.getMetadata().getLabels();
        Objects.requireNonNull(labels);
        labels.put(KuberLabels.LZY_POD_NAME_LABEL, podName);
        pod.getMetadata().setLabels(labels);
        pod.getSpec().setTolerations(GPU_VM_POD_TOLERATIONS);
        final Map<String, String> nodeSelector = Map.of(KuberLabels.NODE_POOL_LABEL, vm.poolId());
        pod.getSpec().setNodeSelector(nodeSelector);
        return pod;
    }

    private V1Pod readPod() {
        final V1Pod pod;
        final File file = new File(config.kuberAllocator().podTemplatePath());
        try {
            pod = (V1Pod) Yaml.load(file);
        } catch (IOException e) {
            LOG.error("IO error while loading yaml file {}", file.getPath());
            throw new RuntimeException("cannot load vm yaml file", e);
        }
        return pod;
    }

    private void buildContainers(Vm vm, V1Pod pod) {
        Objects.requireNonNull(pod.getSpec());
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
    }
}
