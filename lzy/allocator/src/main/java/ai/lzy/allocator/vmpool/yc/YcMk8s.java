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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static yandex.cloud.api.k8s.v1.ClusterOuterClass.Cluster;

@Singleton
@Requires(property = "allocator.yc-mk8s.enabled", value = "true")
public class YcMk8s implements VmPoolRegistry, ClusterRegistry {
    private static final Logger LOG = LogManager.getLogger(YcMk8s.class);

    private final ServiceConfig config;
    private final ClusterServiceBlockingStub clusterServiceClient;
    private final NodeGroupServiceBlockingStub nodeGroupServiceClient;

    // guarded by this
    private final Map<String, VmPoolSpec> systemPools = new HashMap<>();
    private final Map<String, VmPoolSpec> userPools = new HashMap<>();

    private record ClusterDesc(
        String clusterId,
        String folderId,
        String masterInternalAddress,
        String masterExternalAddress,
        String masterCert,
        String clusterIpv4CidrBlock,
        ClusterType type,
        Map<String, VmPoolSpec> pools
    ) {
        @Override
        public String toString() {
            var sb = new StringBuilder()
                .append(type.name()).append(" cluster '").append(clusterId).append("'\n")
                .append("  folder       : ").append(folderId).append('\n')
                .append("  internal addr: ").append(masterInternalAddress).append('\n')
                .append("  external addr: ").append(masterExternalAddress).append('\n')
                .append("  ip4 CIDR     : ").append(clusterIpv4CidrBlock).append('\n')
                .append("  pools        :").append('\n');

            for (var pool : pools.entrySet()) {
                sb.append("    ").append(pool.getKey()).append(": ").append(pool.getValue()).append('\n');
            }

            return sb.toString();
        }
    }

    // guarded by this
    private final Map<String, ClusterDesc> clusters = new HashMap<>();


    @Inject
    public YcMk8s(ServiceConfig config, ServiceFactory serviceFactory) {
        this.config = config;

        this.clusterServiceClient = serviceFactory
            .create(ClusterServiceBlockingStub.class, ClusterServiceGrpc::newBlockingStub);

        this.nodeGroupServiceClient = serviceFactory
            .create(NodeGroupServiceBlockingStub.class, NodeGroupServiceGrpc::newBlockingStub);

        syncClusters();
    }

    @Scheduled(fixedDelay = "${allocator.yc-mk8s.period}", initialDelay = "${allocator.yc-mk8s.period}")
    public void syncClusters() {
        config.getServiceClusters().forEach(clusterId -> addCluster(clusterId, /* system */ true));
        config.getUserClusters().forEach(clusterId -> addCluster(clusterId, /* system */ false));
    }

    @Override
    public synchronized Map<String, VmPoolSpec> getSystemVmPools() {
        return systemPools;
    }

    @Override
    public synchronized Map<String, VmPoolSpec> getUserVmPools() {
        return userPools;
    }

    @Nullable
    @Override
    public synchronized VmPoolSpec findPool(String poolLabel) {
        var poolSpec = systemPools.get(poolLabel);
        if (poolSpec != null) {
            return poolSpec;
        }
        return userPools.get(poolLabel);
    }

    @Override
    @Nullable
    public synchronized ClusterDescription findCluster(String poolLabel, String zone, ClusterType type) {
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
            .filter(c -> c.pools().containsKey(poolLabel))
            .findFirst()
            .orElse(null);

        if (desc == null) {
            return null;
        }
        return toClusterDescription(desc);
    }

    @Override
    public synchronized ClusterDescription getCluster(String clusterId) {
        final var desc = clusters.get(clusterId);
        if (desc == null) {
            throw new NoSuchElementException("cluster with id " + clusterId + " not found");
        }
        return new ClusterDescription(desc.clusterId, desc.masterExternalAddress, desc.masterCert, desc.type,
            convertPools(desc.pools));
    }

    @Override
    public synchronized List<ClusterDescription> listClusters(@Nullable ClusterType clusterType) {
        if (clusterType == null) {
            return clusters.values().stream()
                .map(x -> new ClusterDescription(x.clusterId, x.masterExternalAddress, x.masterCert, x.type,
                    convertPools(x.pools)))
                .toList();
        }
        return clusters.values().stream()
            .filter(c -> c.type() == clusterType)
            .map(YcMk8s::toClusterDescription)
            .toList();
    }

    @Override
    public synchronized String getClusterPodsCidr(String clusterId) {
        ClusterDesc desc = clusters.get(clusterId);
        if (desc == null) {
            throw new NoSuchElementException("cluster with id " + clusterId + " not found");
        }
        return desc.clusterIpv4CidrBlock();
    }

    private void addCluster(String clusterId, boolean system) {
        var newCluster = resolveCluster(clusterId, system);

        if (newCluster == null) {
            LOG.error("Cannot add {} cluster {}", ct(system), clusterId);
            return;
        }

        synchronized (this) {
            var oldCluster = clusters.get(clusterId);
            if (oldCluster != null) {
                if (oldCluster.equals(newCluster)) {
                    return;
                }
                LOG.info("Replace {} cluster\n  from: {}\n  to: {}", ct(system), oldCluster, newCluster);
            } else {
                LOG.info("Add {} cluster: {}", ct(system), newCluster);
            }

            clusters.put(clusterId, newCluster);

            systemPools.clear();
            userPools.clear();

            clusters.values().forEach(cluster -> {
                switch (cluster.type) {
                    case User -> userPools.putAll(cluster.pools());
                    case System -> systemPools.putAll(cluster.pools());
                }
            });
        }
    }

    @Nullable
    private ClusterDesc resolveCluster(String clusterId, boolean system) {
        LOG.debug("Resolve {} cluster {}...", ct(system), clusterId);

        Cluster cluster;
        try {
            cluster = clusterServiceClient.get(
                GetClusterRequest.newBuilder()
                    .setClusterId(clusterId)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Cannot resolve {} cluster {}: {}", ct(system), clusterId, e.getStatus(), e);
            return null;
        }

        if (cluster.getStatus() != Cluster.Status.RUNNING) {
            LOG.warn("Skip {} cluster {} at state {}", ct(system), clusterId, cluster.getStatus());
            return null;
        }

        if (cluster.getHealth() != Cluster.Health.HEALTHY) {
            LOG.error("Configuration error, {} cluster {} is not healthy", ct(system), clusterId);
            return null;
        }

        NodeGroupServiceOuterClass.ListNodeGroupsResponse nodeGroupsResponse;
        try {
            nodeGroupsResponse = nodeGroupServiceClient.list(
                ListNodeGroupsRequest.newBuilder()
                    .setFolderId(cluster.getFolderId())
                    .setPageSize(1000)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("GRPC error while requesting node groups in cluster {}: {}", cluster.getId(), e.getStatus(), e);
            return null;
        }
        assert nodeGroupsResponse.getNextPageToken().isEmpty();

        var clusterDesc = createClusterDesc(cluster, system);

        if (system) {
            return clusterDesc;
        }

        for (var nodeGroup : nodeGroupsResponse.getNodeGroupsList()) {
            if (!clusterId.equals(nodeGroup.getClusterId())) {
                continue;
            }

            if (nodeGroup.getStatus() != NodeGroup.Status.RUNNING) {
                LOG.warn("Skip node group {} ({}) in state {}",
                    nodeGroup.getId(), nodeGroup.getName(), nodeGroup.getStatus());
                continue;
            }
            if (!nodeGroup.getClusterId().equals(clusterId)) {
                continue;
            }

            var label = nodeGroup.getNodeLabelsMap().get(KuberLabels.NODE_POOL_LABEL);
            var zone = nodeGroup.getNodeLabelsMap().get(KuberLabels.NODE_POOL_AZ_LABEL);
            var kind = nodeGroup.getNodeLabelsMap().get(KuberLabels.NODE_POOL_KIND_LABEL);

            if (label == null || zone == null || kind == null) {
                LOG.error("Bad node group: {}", nodeGroup);
                continue;
            }

            var existingVmPool = clusterDesc.pools.get(label);
            if (existingVmPool != null) {
                existingVmPool.zones().add(zone);
            } else {
                var vmPoolSpec = resolveVmPoolSpec(nodeGroup, label, zone);
                clusterDesc.pools.put(label, vmPoolSpec);
            }
        }

        return clusterDesc;
    }

    private static ClusterDesc createClusterDesc(Cluster cluster, boolean system) {
        var clusterId = cluster.getId();
        var master = cluster.getMaster();
        var masterCert = master.getMasterAuth().getClusterCaCertificate();
        var masterInternalAddress = master.getEndpoints().getInternalV4Endpoint();
        var masterExternalAddress = master.getEndpoints().getExternalV6Endpoint().isEmpty() ?
            master.getEndpoints().getExternalV4Endpoint() : master.getEndpoints().getExternalV6Endpoint();

        return new ClusterDesc(
            clusterId,
            cluster.getFolderId(),
            masterInternalAddress,
            masterExternalAddress,
            masterCert,
            cluster.getIpAllocationPolicy().getClusterIpv4CidrBlock(),
            system ? ClusterType.System : ClusterType.User,
            new HashMap<>());
    }

    private static VmPoolSpec resolveVmPoolSpec(NodeGroup nodeGroup, String label, String zone) {
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

        return newVmSpec;
    }

    private static String ct(boolean system) {
        return system ? "system" : "user";
    }

    private static Map<String, PoolType> convertPools(Map<String, VmPoolSpec> pools) {
        return pools.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                kv -> kv.getValue().cpuCount() > 0 ? PoolType.Gpu : PoolType.Cpu
            ));
    }

    private static ClusterDescription toClusterDescription(ClusterDesc desc) {
        var hostAndPort = desc.masterExternalAddress().isEmpty()
            ? desc.masterInternalAddress()
            : desc.masterExternalAddress();
        return new ClusterDescription(desc.clusterId(), hostAndPort, desc.masterCert(), desc.type(),
            convertPools(desc.pools()));
    }
}
