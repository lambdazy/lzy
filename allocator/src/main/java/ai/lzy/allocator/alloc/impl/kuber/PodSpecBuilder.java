package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.volume.HostPathVolumeDescription;
import ai.lzy.allocator.volume.Volume.AccessMode;
import ai.lzy.allocator.volume.VolumeClaim;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator.POD_NAME_PREFIX;

public class PodSpecBuilder {
    private static final String POD_TEMPLATE_PATH = "kubernetes/lzy-vm-pod-template.yaml";
    private static final List<Toleration> GPU_VM_POD_TOLERATION = List.of(
        new TolerationBuilder()
            .withKey("sku")
            .withOperator("Equal")
            .withValue("gpu")
            .withEffect("NoSchedule")
            .build());

    private final Vm.Spec vmSpec;
    private final Pod pod;
    private final ServiceConfig config;
    private final List<Container> containers = new ArrayList<>();
    private final Map<String, Volume> volumes = new HashMap<>();

    public PodSpecBuilder(Vm.Spec vmSpec, KubernetesClient client, ServiceConfig config) {
        this.vmSpec = vmSpec;
        pod = loadPodTemplate(client);
        this.config = config;

        final String podName = POD_NAME_PREFIX + vmSpec.vmId().toLowerCase(Locale.ROOT);

        // k8s pod name can only contain symbols [-a-z0-9]
        pod.getMetadata().setName(podName.replaceAll("[^-a-z0-9]", "-"));
        var labels = pod.getMetadata().getLabels();

        Objects.requireNonNull(labels);
        labels.putAll(Map.of(
            KuberLabels.LZY_POD_NAME_LABEL, podName,
            KuberLabels.LZY_POD_SESSION_ID_LABEL, vmSpec.sessionId()
        ));
        pod.getMetadata().setLabels(labels);

        pod.getSpec().setTolerations(GPU_VM_POD_TOLERATION);

        final Map<String, String> nodeSelector = Map.of(
            KuberLabels.NODE_POOL_LABEL, vmSpec.poolLabel(),
            KuberLabels.NODE_POOL_AZ_LABEL, vmSpec.zone(),
            KuberLabels.NODE_POOL_STATE_LABEL, "ACTIVE"
        );
        pod.getSpec().setNodeSelector(nodeSelector);
    }

    private Pod loadPodTemplate(KubernetesClient client) {
        try (final var stream = Objects.requireNonNull(getClass()
            .getClassLoader()
            .getResourceAsStream(POD_TEMPLATE_PATH)))
        {
            return client.pods()
                .load(stream)
                .get();
        } catch (IOException e) {
            throw new RuntimeException("Error while reading pod template", e);
        }
    }

    public String getPodName() {
        return pod.getMetadata().getName();
    }

    public PodSpecBuilder withWorkloads(List<Workload> workloads) {
        for (var workload : workloads) {
            final var container = new Container();
            final var envList = workload.env().entrySet()
                .stream()
                .map(e -> new EnvVarBuilder()
                    .withName(e.getKey())
                    .withValue(e.getValue())
                    .build()
                )
                .collect(Collectors.toList());

            envList.addAll(List.of(
                new EnvVarBuilder()
                    .withName(AllocatorAgent.VM_ALLOCATOR_ADDRESS)
                    .withValue(config.getAddress())
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.VM_ID_KEY)
                    .withValue(vmSpec.vmId())
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.VM_HEARTBEAT_PERIOD)
                    .withValue(config.getHeartbeatTimeout().dividedBy(2).toString())
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.VM_IP_ADDRESS)
                    .withValueFrom(
                        new EnvVarSourceBuilder()
                            .withNewFieldRef("v1", "status.podIP")
                            .build()
                    )
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

            container.setVolumeMounts(workload.mounts().stream().map(
                volumeMount ->
                    new VolumeMountBuilder()
                        .withName(volumeMount.name())
                        .withMountPath(volumeMount.path())
                        .withReadOnly(volumeMount.readOnly())
                        .withMountPropagation(volumeMount.mountPropagation().asString())
                        .build()
            ).toList());

            final var context = new SecurityContext();
            context.setPrivileged(true);
            context.setRunAsUser(0L);
            container.setSecurityContext(context);

            containers.add(container);
        }
        return this;
    }

    public PodSpecBuilder withVolumes(List<VolumeClaim> volumeClaims) {
        for (var volumeClaim : volumeClaims) {
            final var volume = new VolumeBuilder()
                .withName(volumeClaim.volumeName())
                .withPersistentVolumeClaim(
                    new PersistentVolumeClaimVolumeSource(
                        volumeClaim.name(),
                        volumeClaim.accessMode() == AccessMode.READ_ONLY_MANY))
                .build();
            final String volumeRequestName = volumeClaim.volumeRequestName();
            if (volumes.containsKey(volumeRequestName)) {
                throw new IllegalArgumentException("Two volumes with the same name " + volumeRequestName);
            }
            volumes.put(volumeRequestName, volume);
        }
        return this;
    }

    public PodSpecBuilder withHostVolumes(List<HostPathVolumeDescription> volumeRequests) {
        for (var request: volumeRequests) {
            final var volume = new VolumeBuilder()
                .withName(request.volumeId())
                .withHostPath(new HostPathVolumeSourceBuilder()
                    .withPath(request.path())
                    .withType(request.hostPathType().asString())
                    .build())
                .build();
            volumes.put(request.name(), volume);
        }
        return this;
    }

    public Pod build() {
        for (var container: containers) {
            container.setVolumeMounts(
                container.getVolumeMounts().stream()
                    .map(volumeMount -> new VolumeMountBuilder()
                        .withName(volumes.get(volumeMount.getName()).getName())
                        .withMountPath(volumeMount.getMountPath())
                        .withReadOnly(volumeMount.getReadOnly())
                        .withMountPropagation(volumeMount.getMountPropagation())
                        .build())
                    .toList()
            );
        }

        pod.getSpec().setContainers(containers);
        pod.getSpec().setVolumes(volumes.values().stream().toList());
        return pod;
    }
}
