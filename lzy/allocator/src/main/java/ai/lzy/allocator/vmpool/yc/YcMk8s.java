package ai.lzy.allocator.vmpool.yc;

import ai.lzy.allocator.alloc.impl.kuber.KuberLabels;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.vmpool.*;
import com.google.common.net.HostAndPort;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.annotation.Requires;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import static yandex.cloud.api.k8s.v1.ClusterOuterClass.Cluster;

@Singleton
@Requires(property = "allocator.yc-mk8s.enabled", value = "true")
public class YcMk8s implements VmPoolRegistry, ClusterRegistry {
    private static final Logger LOG = LogManager.getLogger(YcMk8s.class);

    private final ServiceConfig config;
    private final ClusterServiceBlockingStub clusterServiceClient;
    private final NodeGroupServiceBlockingStub nodeGroupServiceClient;

    private final Map<String, VmPoolSpec> systemPools = new HashMap<>();
    private final Map<String, VmPoolSpec> userPools = new HashMap<>();

    private final Set<String> systemFolders = new HashSet<>();
    private final Set<String> userFolders = new HashSet<>();

    private final Map<String, Set<String>> folder2clusters = new HashMap<>();

    private record NodeGroupDesc(
        String zone,
        String label,
        NodeGroup proto
    ) {}

    private record ClusterDesc(
        String clusterId,
        String folderId,
        HostAndPort masterInternalAddress,
        HostAndPort masterExternalAddress,
        String masterCert,
        Map<String, NodeGroupDesc> nodeGroups,
        String clusterIpv4CidrBlock,
        ClusterType type
    ) {}

    private final Map<String, ClusterDesc> clusters = new HashMap<>();


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
        if (systemPools.containsKey(poolLabel)) {
            return systemPools.get(poolLabel);
        }
        if (userPools.containsKey(poolLabel)) {
            return userPools.get(poolLabel);
        }
        return null;
    }

    @Override
    @Nullable
    public ClusterDescription findCluster(String poolLabel, String zone, ClusterType type) {
        // TODO(artolord) make better logic of vm scheduling
        final var desc = clusters.values()
            .stream()
            .filter(cluster -> cluster.type.equals(type))
            .filter(cluster -> cluster.nodeGroups.values().stream()
                .anyMatch(ng -> ng.zone.equals(zone) && ng.label.equals(poolLabel)))
            .findFirst()
            .orElse(null);

        if (desc == null) {
            return null;
        }
        return new ClusterDescription(desc.clusterId, desc.masterExternalAddress, desc.masterCert, desc.type);
    }

    @Override
    public ClusterDescription getCluster(String clusterId) {
        final var desc = clusters.get(clusterId);
        if (desc == null) {
            throw new NoSuchElementException("cluster with id " + clusterId + " not found");
        }
        return new ClusterDescription(desc.clusterId, desc.masterExternalAddress, desc.masterCert, desc.type);
    }

    @Override
    public String getClusterPodsCidr(String clusterId) {
        ClusterDesc desc = clusters.get(clusterId);
        if (desc == null) {
            throw new NoSuchElementException("cluster with id " + clusterId + " not found");
        }
        return desc.clusterIpv4CidrBlock();
    }

    // TODO: getters for YC-specific data

    private void resolveCluster(String clusterId, boolean system) {
        LOG.info("Resolve {} cluster {}...", ct(system), clusterId);

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

        var master = cluster.getMaster();
        var masterCert = master.getMasterAuth().getClusterCaCertificate();
        var masterInternalAddress = master.hasZonalMaster()
            ? master.getZonalMaster().getInternalV4Address()
            : master.getRegionalMaster().getInternalV4Address();
        var masterExternalAddress = master.hasZonalMaster()
            ? master.getZonalMaster().getExternalV4Address()
            : master.getRegionalMaster().getExternalV4Address();

        LOG.info("""
            Resolved {} cluster {}:
              id: {}
              folder_id: {}
              name: {}
              description: {}
              k8s-master-type: {}
              k8s-master-internal: {}
              k8s-master-external: {}
              k83-master-cert: {}
            """,
            ct(system), clusterId, cluster.getId(), cluster.getFolderId(), cluster.getName(), cluster.getDescription(),
            master.getMasterTypeCase(), masterInternalAddress, masterExternalAddress, /* masterCert */ "***");

        folder2clusters.computeIfAbsent(cluster.getFolderId(), x -> new HashSet<>()).add(clusterId);
        (system ? systemFolders : userFolders).add(cluster.getFolderId());

        var clusterDesc = new ClusterDesc(
            clusterId,
            cluster.getFolderId(),
            HostAndPort.fromString(masterInternalAddress),
            HostAndPort.fromString(masterExternalAddress),
            masterCert,
            new HashMap<>(),
            cluster.getIpAllocationPolicy().getClusterIpv4CidrBlock(),
            system ? ClusterType.System : ClusterType.User);
        clusters.put(clusterId, clusterDesc);

        // process node groups

        NodeGroupServiceOuterClass.ListNodeGroupsResponse nodeGroups;
        try {
            nodeGroups = nodeGroupServiceClient.list(
                ListNodeGroupsRequest.newBuilder()
                    .setFolderId(cluster.getFolderId())
                    .setPageSize(1000)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("GRPC error while requesting node groups in cluster {}: {}", cluster.getId(), e.getStatus(), e);
            return;
        }

        // TODO
        assert nodeGroups.getNextPageToken().isEmpty();

        for (var nodeGroup : nodeGroups.getNodeGroupsList()) {
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
                nodeGroup.getId(), nodeGroup.getName(), cluster.getFolderId(), clusterId, zone, label,
                nodeTemplate.getPlatformId(), spec.getCores(), cpuType, spec.getGpus(), gpuType, spec.getMemory());

            var nodeGroupDesc = new NodeGroupDesc(zone, label, nodeGroup);
            clusterDesc.nodeGroups().put(nodeGroup.getId(), nodeGroupDesc);

            var pool = system ? systemPools : userPools;
            var vmSpec = pool.get(label);
            if (vmSpec == null) {
                vmSpec = new VmPoolSpec(label, cpuType, (int) spec.getCores(), gpuType, (int) spec.getGpus(),
                    (int) (spec.getMemory() >> 30), new HashSet<>());
                pool.put(label, vmSpec);
            }
            vmSpec.zones().add(zone);
        }
    }

    private static String ct(boolean system) {
        return system ? "system" : "user";
    }
}
