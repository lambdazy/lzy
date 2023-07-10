package ai.lzy.allocator.vmpool.yc;

import ai.lzy.allocator.alloc.impl.kuber.KuberLabels;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.vmpool.CpuTypes;
import ai.lzy.allocator.vmpool.GpuTypes;
import ai.lzy.allocator.vmpool.VmPoolRegistry;
import ai.lzy.allocator.vmpool.VmPoolSpec;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.k8s.v1.ClusterServiceGrpc;
import yandex.cloud.api.k8s.v1.ClusterServiceGrpc.ClusterServiceBlockingStub;
import yandex.cloud.api.k8s.v1.ClusterServiceOuterClass.GetClusterRequest;
import yandex.cloud.api.k8s.v1.NodeGroupOuterClass.NodeGroup;
import yandex.cloud.api.k8s.v1.NodeGroupServiceGrpc;
import yandex.cloud.api.k8s.v1.NodeGroupServiceGrpc.NodeGroupServiceBlockingStub;
import yandex.cloud.api.k8s.v1.NodeGroupServiceOuterClass;
import yandex.cloud.api.k8s.v1.NodeGroupServiceOuterClass.ListNodeGroupsRequest;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.grpc.interceptors.RequestIdInterceptor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static yandex.cloud.api.k8s.v1.ClusterOuterClass.Cluster;

@Singleton
@Requires(property = "allocator.yc-mk8s.enabled", value = "true")
public class YcMk8s implements VmPoolRegistry, ClusterRegistry {
    private static final Logger LOG = LogManager.getLogger(YcMk8s.class);

    private final ServiceConfig config;
    private final ClusterServiceBlockingStub clusterServiceClient;
    private final NodeGroupServiceBlockingStub nodeGroupServiceClient;

    private final Map<String, VmPoolSpec> systemPools = new ConcurrentHashMap<>();
    private final Map<String, VmPoolSpec> userPools = new ConcurrentHashMap<>();

    private final Map<String, Set<String>> folder2clusters = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> cluster2labels = new ConcurrentHashMap<>();

    private record ClusterDesc(
        String clusterId,
        String folderId,
        String masterInternalAddress,
        String masterExternalAddress,
        String masterCert,
        String clusterIpv4CidrBlock,
        ClusterType type
    )
    {
    }

    private final Map<String, ClusterDesc> clusters = new ConcurrentHashMap<>();


    @Inject
    public YcMk8s(ServiceConfig config, ServiceFactory serviceFactory) {
        this.config = config;

        this.clusterServiceClient = serviceFactory
            .create(ClusterServiceBlockingStub.class, ClusterServiceGrpc::newBlockingStub)
            .withInterceptors(
                // TODO: forward X-REQUEST-ID header
                new RequestIdInterceptor());

        this.nodeGroupServiceClient = serviceFactory
            .create(NodeGroupServiceBlockingStub.class, NodeGroupServiceGrpc::newBlockingStub)
            .withInterceptors(
                // TODO: forward X-REQUEST-ID header
                new RequestIdInterceptor());
    }

    @Scheduled(fixedDelay = "${allocator.yc-mk8s.period}")
    public void syncClusters() {
        config.getServiceClusters().forEach(clusterId -> resolveCluster(clusterId, /* system */ true));
        config.getUserClusters().forEach(clusterId -> resolveCluster(clusterId, /* system */ false));
    }


    @Override
    public Map<String, VmPoolSpec> getSystemVmPools() {
        return systemPools;
    }

    @Override
    public Map<String, VmPoolSpec> getUserVmPools() {
        return userPools;
    }

    @Nullable
    @Override
    public VmPoolSpec findPool(String poolLabel) {
        VmPoolSpec poolSpec;
        poolSpec = systemPools.get(poolLabel);
        if (poolSpec != null) {
            return poolSpec;
        }
        poolSpec = userPools.get(poolLabel);
        return poolSpec;
    }

    @Override
    @Nullable
    public ClusterDescription findCluster(String poolLabel, String zone, ClusterType type) {
        // TODO(artolord) make better logic of vm scheduling

        final var pools = switch (type) {
            case System -> systemPools;
            case User -> userPools;
        };
        final var poolSpec = pools.get(poolLabel);

        if (poolSpec == null || !poolSpec.zones().contains(zone)) {
            return null;
        }

        final var desc = clusters.values().stream()
            .filter(c -> cluster2labels.get(c.clusterId()).contains(poolLabel))
            .findFirst()
            .orElse(null);

        if (desc == null) {
            return null;
        }
        return toClusterDescription(desc);
    }

    @Override
    public ClusterDescription getCluster(String clusterId) {
        final var desc = clusters.get(clusterId);
        if (desc == null) {
            throw new NoSuchElementException("cluster with id " + clusterId + " not found");
        }
        return toClusterDescription(desc);
    }

    @Override
    public String getClusterPodsCidr(String clusterId) {
        ClusterDesc desc = clusters.get(clusterId);
        if (desc == null) {
            throw new NoSuchElementException("cluster with id " + clusterId + " not found");
        }
        return desc.clusterIpv4CidrBlock();
    }

    @Override
    public List<ClusterDescription> listClusters(ClusterType clusterType) {
        return clusters.values().stream()
            .filter(c -> c.type() == clusterType)
            .map(this::toClusterDescription)
            .toList();
    }

    // TODO: getters for YC-specific data

    private ClusterDescription toClusterDescription(ClusterDesc desc) {
        var hostAndPort =
            desc.masterExternalAddress().isEmpty() ? desc.masterInternalAddress() :
                desc.masterExternalAddress();
        return new ClusterDescription(desc.clusterId(), hostAndPort, desc.masterCert(), desc.type());
    }

    private void resolveCluster(String clusterId, boolean system) {
        LOG.debug("Resolve {} cluster {}...", ct(system), clusterId);

        Cluster cluster;
        try {
            cluster = clusterServiceClient.get(
                GetClusterRequest.newBuilder()
                    .setClusterId(clusterId)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Cannot resolve {} cluster {}: {}", ct(system), clusterId, e.getStatus(), e);
            throw new RuntimeException(e);
        }

        if (cluster.getStatus() != Cluster.Status.RUNNING) {
            LOG.warn("Skip {} cluster {} at state {}", ct(system), clusterId, cluster.getStatus());
            return;
        }

        if (cluster.getHealth() != Cluster.Health.HEALTHY) {
            var msg = "Configuration error, %s cluster %s is not healthy".formatted(ct(system), clusterId);
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        resolveClusterDescription(cluster, system);

        // process node groups

        NodeGroupServiceOuterClass.ListNodeGroupsResponse nodeGroupsResponse;
        try {
            nodeGroupsResponse = nodeGroupServiceClient.list(
                ListNodeGroupsRequest.newBuilder()
                    .setFolderId(cluster.getFolderId())
                    .setPageSize(1000)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("GRPC error while requesting node groups in cluster {}: {}", cluster.getId(), e.getStatus(), e);
            return;
        }

        // TODO
        assert nodeGroupsResponse.getNextPageToken().isEmpty();

        final Set<String> clusterVmSpecLabels = new HashSet<>();
        for (var nodeGroup : nodeGroupsResponse.getNodeGroupsList()) {
            if (!clusterId.equals(nodeGroup.getClusterId())) {
                continue;
            }

            if (nodeGroup.getStatus() != NodeGroup.Status.RUNNING) {
                LOG.info("Skip node group {} ({}) in state {}",
                    nodeGroup.getId(), nodeGroup.getName(), nodeGroup.getStatus());
                continue;
            }
            if (!nodeGroup.getClusterId().equals(clusterId)) {
                continue;
            }

            var label = nodeGroup.getNodeLabelsMap().get(KuberLabels.NODE_POOL_LABEL);
            var zone = nodeGroup.getNodeLabelsMap().get(KuberLabels.NODE_POOL_AZ_LABEL);

            if (label == null || zone == null) {  // Skip old node groups
                continue;
            }

            resolveVmPoolSpec(cluster, nodeGroup, label, zone, system);
            clusterVmSpecLabels.add(label);
        }

        cluster2labels.computeIfAbsent(clusterId, __ -> new HashSet<>()).forEach(label -> {
            if (!clusterVmSpecLabels.contains(label)) {
                (system ? systemPools : userPools).remove(label);
            }
        });
        cluster2labels.put(clusterId, clusterVmSpecLabels);
    }

    private ClusterDesc resolveClusterDescription(Cluster cluster, boolean system) {
        var clusterId = cluster.getId();
        var master = cluster.getMaster();
        var masterCert = master.getMasterAuth().getClusterCaCertificate();
        var masterInternalAddress = master.getEndpoints().getInternalV4Endpoint();
        var masterExternalAddress = master.getEndpoints().getExternalV6Endpoint().isEmpty() ?
            master.getEndpoints().getExternalV4Endpoint() : master.getEndpoints().getExternalV6Endpoint();

        var clusterNewDesc = new ClusterDesc(
            clusterId,
            cluster.getFolderId(),
            masterInternalAddress,
            masterExternalAddress,
            masterCert,
            cluster.getIpAllocationPolicy().getClusterIpv4CidrBlock(),
            system ? ClusterType.System : ClusterType.User);

        ClusterDesc clusterOldDesc = clusters.get(clusterId);

        if (clusterOldDesc != null && clusterOldDesc.equals(clusterNewDesc)) {
            LOG.debug("Resolved old cluster {}", clusterId);
            return clusterOldDesc;
        }

        LOG.info("""
                Resolved {} new cluster {}:
                  id: {}
                  folder_id: {}
                  name: {}
                  description: {}
                  k8s-master-type: {}
                  k8s-master-internal: {}
                  k8s-master-external: {}
                  k83-master-cert: {}
                """,
            ct(system), clusterId, cluster.getId(), cluster.getFolderId(), cluster.getName(),
            cluster.getDescription(),
            master.getMasterTypeCase(), masterInternalAddress, masterExternalAddress, /* masterCert */ "***");

        folder2clusters.computeIfAbsent(cluster.getFolderId(), x -> new HashSet<>()).add(clusterId);
        clusters.put(clusterId, clusterNewDesc);
        return clusterNewDesc;
    }

    private void resolveVmPoolSpec(Cluster cluster, NodeGroup nodeGroup,
                                   String label, String zone, boolean system)
    {
        var nodeTemplate = nodeGroup.getNodeTemplate();
        var spec = nodeTemplate.getResourcesSpec();

        var platform = nodeTemplate.getPlatformId();
        final String cpuType;
        final String gpuType;

        switch (platform) {
            case "standard-v1" -> {
                cpuType = CpuTypes.BROADWELL.value();
                gpuType = GpuTypes.NO_GPU.value();
            }
            case "standard-v2" -> {
                cpuType = CpuTypes.CASCADE_LAKE.value();
                gpuType = GpuTypes.NO_GPU.value();
            }
            case "standard-v3" -> {
                cpuType = CpuTypes.ICE_LAKE.value();
                gpuType = GpuTypes.NO_GPU.value();
            }
            case "gpu-standard-v1" -> {
                cpuType = CpuTypes.BROADWELL.value();
                gpuType = GpuTypes.V100.value();
            }
            case "gpu-standard-v2" -> {
                cpuType = CpuTypes.CASCADE_LAKE.value();
                gpuType = GpuTypes.V100.value();
            }
            case "standard-v3-t4" -> {
                cpuType = CpuTypes.ICE_LAKE.value();
                gpuType = GpuTypes.T4.value();
            }
            case "gpu-standard-v3" -> {
                cpuType = CpuTypes.AMD_EPYC.value();
                gpuType = GpuTypes.A100.value();
            }
            default -> {
                LOG.error("Cannot resolve platform {} for pool {}", platform, label);
                throw new RuntimeException("Cannot resolve platform for pool " + label);
            }
        }

        var newVmSpec = new VmPoolSpec(label, cpuType, (int) spec.getCores(), gpuType, (int) spec.getGpus(),
            (int) (spec.getMemory() >> 30), new HashSet<>());
        newVmSpec.zones().add(zone);

        var pool = system ? systemPools : userPools;
        var oldVmSpec = pool.get(label);

        if (oldVmSpec != null && oldVmSpec.equals(newVmSpec)) {
            LOG.debug("Resolved old node group {}", nodeGroup.getId());
            return;
        }

        LOG.info("""
                Resolved node group {} ({}):
                  folder_id: {}
                  cluster_id: {}
                  zone: {}
                  label: {}
                  platform: {}
                  cpu: {} ({})
                  gpu: {} ({})
                  ram: {}
                """,
            nodeGroup.getId(), nodeGroup.getName(), cluster.getFolderId(), cluster.getId(), zone, label,
            nodeTemplate.getPlatformId(), spec.getCores(), cpuType, spec.getGpus(), gpuType, spec.getMemory());

        pool.put(label, newVmSpec);
    }

    private static String ct(boolean system) {
        return system ? "system" : "user";
    }
}
