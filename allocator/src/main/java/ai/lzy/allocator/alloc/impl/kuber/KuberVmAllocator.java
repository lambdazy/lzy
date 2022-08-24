package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URISyntaxException;
import java.util.*;

@Singleton
@Requires(property = "allocator.kuber-allocator.enabled", value = "true")
public class KuberVmAllocator implements VmAllocator {
    private static final Logger LOG = LogManager.getLogger(KuberVmAllocator.class);
    private static final String NAMESPACE = "default";
    private static final List<Toleration> GPU_VM_POD_TOLERATIONS = List.of(
        new TolerationBuilder()
            .withKey("sku")
            .withOperator("Equal")
            .withValue("gpu")
            .withEffect("NoSchedule")
            .build());

    private static final String NAMESPACE_KEY = "namespace";
    private static final String POD_NAME_KEY = "pod-name";
    private static final String CLUSTER_ID_KEY = "cluster-id";
    public static final String POD_NAME_PREFIX = "lzy-vm-";

    private final VmDao dao;
    private final ClusterRegistry poolRegistry;
    private final KuberClientFactory factory;
    private final ServiceConfig config;

    @Inject
    public KuberVmAllocator(VmDao dao, ClusterRegistry poolRegistry, KuberClientFactory factory, ServiceConfig config) {
        this.dao = dao;
        this.poolRegistry = poolRegistry;
        this.factory = factory;
        this.config = config;
    }

    @Override
    public void allocate(Vm vm) throws InvalidConfigurationException {
        final var cluster = poolRegistry.findCluster(vm.poolLabel(), vm.zone(), ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + vm.poolLabel() + " and zone " + vm.zone());
        }

        try (final var client = factory.build(cluster)) {
            final Pod vmPodSpec = createVmPodSpec(vm, client);
            LOG.debug("Creating pod with podspec: {}", vmPodSpec);
            dao.saveAllocatorMeta(vm.vmId(), Map.of(
                NAMESPACE_KEY, NAMESPACE,
                POD_NAME_KEY, vmPodSpec.getMetadata().getName(),
                CLUSTER_ID_KEY, cluster.clusterId()
            ), null);

            final Pod pod;
            try {
                pod = client.pods()
                    .inNamespace(NAMESPACE)
                    .resource(vmPodSpec)
                    .create();
            } catch (Exception e) {
                LOG.error("Failed to allocate pod", e);
                deallocate(vm);
                //TODO (tomato): add retries here if the error is caused due to temporal problems with kuber
                throw new RuntimeException(e);
            }
            LOG.debug("Created pod in Kuber: {}", pod);
        }
    }

    @Nullable
    private Pod getPod(String namespace, String name, KubernetesClient client) {
        final var podsList = client.pods()
            .inNamespace(namespace)
            .list(new ListOptionsBuilder()
                .withLabelSelector(KuberLabels.LZY_POD_NAME_LABEL + "=" + name)
                .build()
            ).getItems();
        if (podsList.size() < 1) {
            return null;
        }
        final var podSpec = podsList.get(0);
        if (podSpec.getMetadata() != null
            && podSpec.getMetadata().getName() != null
            && podSpec.getMetadata().getName().equals(name))
        {
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

        final var clusterId = meta.get(CLUSTER_ID_KEY);
        final var credentials = poolRegistry.getCluster(clusterId);
        final var ns = meta.get(NAMESPACE_KEY);
        final var podName = meta.get(POD_NAME_KEY);
        try (final var client = factory.build(credentials)) {
            final var pod = getPod(ns, podName, client);
            if (pod != null) {
                client.pods()
                    .inNamespace(ns)
                    .resource(pod)
                    .delete();
            } else {
                LOG.warn("Pod with name {} not found", podName);
            }
        }
    }

    public Pod createVmPodSpec(Vm vm, KubernetesClient client) {

        final Pod pod = readPod(client);

        pod.getSpec().setContainers(buildContainers(vm));

        final String podName = POD_NAME_PREFIX + vm.vmId().toLowerCase(Locale.ROOT);
        // k8s pod name can only contain symbols [-a-z0-9]
        pod.getMetadata().setName(podName.replaceAll("[^-a-z0-9]", "-"));
        var labels = pod.getMetadata().getLabels();

        Objects.requireNonNull(labels);
        labels.putAll(Map.of(
            KuberLabels.LZY_POD_NAME_LABEL, podName,
            KuberLabels.LZY_POD_SESSION_ID_LABEL, vm.sessionId()
        ));
        pod.getMetadata().setLabels(labels);

        pod.getSpec().setTolerations(GPU_VM_POD_TOLERATIONS);

        final Map<String, String> nodeSelector = Map.of(
            KuberLabels.NODE_POOL_LABEL, vm.poolLabel(),
            KuberLabels.NODE_POOL_AZ_LABEL, vm.zone(),
            KuberLabels.NODE_POOL_STATE_LABEL, "ACTIVE"
        );
        pod.getSpec().setNodeSelector(nodeSelector);

        return pod;
    }

    private Pod readPod(KubernetesClient client) {
        final File file;
        try {
            file = new File(Objects.requireNonNull(getClass()
                    .getClassLoader()
                    .getResource("kubernetes/lzy-vm-pod-template.yaml"))
                .toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException("Error while reading pod template", e);
        }
        return client.pods()
            .load(file)
            .get();
    }

    private List<Container> buildContainers(Vm vm) {
        final List<Container> containers = new ArrayList<>();
        for (var workload : vm.workloads()) {
            final var container = new Container();
            final var envList = workload.env().entrySet()
                .stream()
                .map(e -> new EnvVarBuilder()
                    .withName(e.getKey())
                    .withName(e.getValue())
                    .build()
                )
                .toList();

            envList.addAll(List.of(
                new EnvVarBuilder()
                    .withName(AllocatorAgent.VM_ALLOCATOR_ADDRESS)
                    .withValue(config.getAddress())
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.VM_ID_KEY)
                    .withValue(vm.vmId())
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.VM_HEARTBEAT_PERIOD)
                    .withValue(config.getHeartbeatTimeout().dividedBy(2).toString())
                    .build()
            ));

            container.setEnv(envList);

            container.setArgs(workload.args());
            container.setName(workload.name());
            container.setImage(workload.image());

            container.setPorts(
                workload.portBindings()
                    .entrySet()
                    .stream()
                    .map(e -> new ContainerPortBuilder()
                        .withContainerPort(e.getKey())
                        .withHostPort(e.getValue())
                        .build())
                    .toList()
            );

            final var context = new SecurityContext();
            context.setPrivileged(true);
            context.setRunAsUser(0L);
            container.setSecurityContext(context);

            containers.add(container);
        }

        return containers;
    }
}
