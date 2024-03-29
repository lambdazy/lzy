package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.HostPathVolumeDescription;
import ai.lzy.allocator.model.Volume.AccessMode;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.model.Workload;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PodSpecBuilder {
    public static final String VM_POD_TEMPLATE_PATH = "kubernetes/lzy-vm-pod-template.yaml";
    public static final String TUNNEL_POD_TEMPLATE_PATH = "kubernetes/lzy-tunnel-pod-template.yaml";
    public static final String MOUNT_HOLDER_POD_TEMPLATE_PATH = "kubernetes/lzy-mount-holder-pod-template.yaml";

    public static final String AFFINITY_TOPOLOGY_KEY = "kubernetes.io/hostname";

    private final Pod pod;
    private final ServiceConfig config;
    private final List<Container> containers = new ArrayList<>();
    private final List<Container> initContainers = new ArrayList<>();
    private final Map<String, Volume> volumes = new HashMap<>();
    private final Map<String, VolumeMount> additionalVolumeMounts = new HashMap<>();
    private final List<PodAffinityTerm> podAffinityTerms = new ArrayList<>();
    private final List<PodAffinityTerm> podAntiAffinityTerms = new ArrayList<>();
    private final List<EnvVar> workloadsEnvList = new ArrayList<>();

    public PodSpecBuilder(String podName, String templatePath, KubernetesClient client, ServiceConfig config) {
        pod = loadPodTemplate(client, templatePath);

        // k8s pod name can only contain symbols [-a-z0-9]
        final var name = podName.replaceAll("[^-a-z0-9]", "-");
        pod.getMetadata().setName(name);
        pod.getMetadata().setUid(name); // TODO: required or not?

        this.config = config;

        pod.getSpec().setAutomountServiceAccountToken(false);
    }

    private Pod loadPodTemplate(KubernetesClient client, String templatePath) {
        try (final var stream = Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(templatePath))) {
            return client.pods().load(stream).get();
        } catch (IOException e) {
            throw new RuntimeException("Error while reading pod template " + templatePath, e);
        }
    }

    public String getPodName() {
        return pod.getMetadata().getName();
    }

    public PodSpecBuilder withLabels(Map<String, String> labels) {
        var podLabels = pod.getMetadata().getLabels();
        podLabels.putAll(labels);
        pod.getMetadata().setLabels(podLabels);
        return this;
    }

    public PodSpecBuilder withTolerations(List<Toleration> tolerations) {
        pod.getSpec().setTolerations(tolerations);
        return this;
    }

    public PodSpecBuilder withNodeSelector(Map<String, String> nodeSelector) {
        pod.getSpec().getNodeSelector().putAll(nodeSelector);
        return this;
    }

    public PodSpecBuilder withEnvVars(Map<String, String> envList) {
        envList.forEach((name, value) -> workloadsEnvList.add(
            new EnvVarBuilder()
                .withName(name)
                .withValue(value)
                .build()
        ));
        return this;
    }

    public PodSpecBuilder withWorkloads(List<Workload> workloads, boolean init) {
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
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.VM_NODE_IP_ADDRESS)
                    .withValueFrom(
                        new EnvVarSourceBuilder()
                            .withNewFieldRef("v1", "status.hostIP")
                            .build()
                    )
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.K8S_POD_NAME)
                    .withValueFrom(
                        new EnvVarSourceBuilder()
                            .withNewFieldRef("v1", "metadata.name")
                            .build()
                    )
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.K8S_NAMESPACE)
                    .withValueFrom(
                        new EnvVarSourceBuilder()
                            .withNewFieldRef("v1", "metadata.namespace")
                            .build()
                    )
                    .build(),
                new EnvVarBuilder()
                    .withName(AllocatorAgent.K8S_CONTAINER_NAME)
                    .withValue(workload.name())
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

            if (init) {
                initContainers.add(container);
            } else {
                containers.add(container);
            }
        }
        return this;
    }

    public PodSpecBuilder withLoggingVolume() {
        final var volumeName = "varloglzy";
        if (volumes.containsKey(volumeName)) {
            return this;
        }
        final var volumePath = "/var/log/lzy";
        final var volume = new VolumeBuilder()
            .withName(volumeName)
            .withHostPath(new HostPathVolumeSource(volumePath, "DirectoryOrCreate"))
            .build();
        final var mount = new VolumeMountBuilder()
            .withName(volumeName)
            .withMountPath(volumePath)
            .build();
        volumes.put(volumeName, volume);
        additionalVolumeMounts.put(volumeName, mount);
        return this;
    }

    public PodSpecBuilder withEmptyDirVolume(String name, String path, EmptyDirVolumeSource emptyDir) {
        final var volume = new VolumeBuilder()
            .withName(name)
            .withEmptyDir(emptyDir)
            .build();

        final var mount = new VolumeMountBuilder()
            .withName(name)
            .withMountPath(path)
            .build();

        if (volumes.containsKey(name)) {
            throw new IllegalArgumentException("Two volumes with the same name " + name);
        }
        volumes.put(name, volume);
        additionalVolumeMounts.put(name, mount);

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

    public PodSpecBuilder withDynamicVolumes(List<DynamicMount> volumeMounts) {
        for (DynamicMount volumeMount : volumeMounts) {
            final var volume = new VolumeBuilder()
                .withName(volumeMount.id())
                .withPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource(volumeMount.volumeClaimName(), false))
                .build();
            final String volumeRequestName = volumeMount.id();
            if (volumes.containsKey(volumeRequestName)) {
                throw new IllegalArgumentException("Two volumes with the same name " + volumeRequestName);
            }
            volumes.put(volumeRequestName, volume);
        }
        return this;
    }

    public PodSpecBuilder withHostVolumes(List<VolumeRequest> volumeRequests) {
        for (var request : volumeRequests) {
            if (!(request.volumeDescription() instanceof HostPathVolumeDescription descr)) {
                continue;
            }
            final var volume = new VolumeBuilder()
                .withName(request.volumeId())
                .withHostPath(new HostPathVolumeSourceBuilder()
                    .withPath(descr.path())
                    .withType(descr.hostPathType().asString())
                    .build())
                .build();
            volumes.put(descr.name(), volume);
        }
        return this;
    }

    public PodSpecBuilder withPodAffinity(String key, String operator, String... values) {
        podAffinityTerms.add(
            new PodAffinityTermBuilder()
                .withLabelSelector(
                    new LabelSelectorBuilder()
                        .withMatchExpressions(
                            new LabelSelectorRequirementBuilder()
                                .withKey(key)
                                .withOperator(operator)
                                .withValues(values)
                                .build()
                        ).build()
                ).withTopologyKey(AFFINITY_TOPOLOGY_KEY)
                .build()
        );
        return this;
    }

    public PodSpecBuilder withPodAntiAffinity(String key, String operator, String... values) {
        podAntiAffinityTerms.add(
            new PodAffinityTermBuilder()
                .withLabelSelector(
                    new LabelSelectorBuilder()
                        .withMatchExpressions(
                            new LabelSelectorRequirementBuilder()
                                .withKey(key)
                                .withOperator(operator)
                                .withValues(values)
                                .build()
                        ).build()
                ).withTopologyKey(AFFINITY_TOPOLOGY_KEY)
                .build()
        );
        return this;
    }

    public Pod build() {
        for (var container : containers) {
            List<VolumeMount> mounts = container.getVolumeMounts().stream()
                .map(volumeMount -> new VolumeMountBuilder()
                    .withName(volumes.get(volumeMount.getName()).getName())
                    .withMountPath(volumeMount.getMountPath())
                    .withReadOnly(volumeMount.getReadOnly())
                    .withMountPropagation(volumeMount.getMountPropagation())
                    .build())
                .collect(Collectors.toList());
            additionalVolumeMounts.forEach((__, mount) -> mounts.add(mount));
            container.setVolumeMounts(mounts);

            var envList = container.getEnv();
            envList.addAll(workloadsEnvList);
            container.setEnv(envList);
        }

        pod.getSpec().setContainers(containers);
        pod.getSpec().setInitContainers(initContainers);
        pod.getSpec().setVolumes(volumes.values().stream().toList());
        pod.getSpec().setAffinity(
            new AffinityBuilder().withPodAffinity(
                new PodAffinityBuilder().addAllToRequiredDuringSchedulingIgnoredDuringExecution(
                    podAffinityTerms
                ).build()
            ).withPodAntiAffinity(
                new PodAntiAffinityBuilder().addAllToRequiredDuringSchedulingIgnoredDuringExecution(
                    podAntiAffinityTerms
                ).build()
            ).build()
        );

        return pod;
    }
}
