package ai.lzy.allocator.vmpool.yc;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.vmpool.VmPoolRegistry;
import ai.lzy.allocator.vmpool.VmPoolSpec;
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

import javax.inject.Singleton;
import java.util.*;

import static yandex.cloud.api.k8s.v1.ClusterOuterClass.Cluster;

@Singleton
@Requires(property = "allocator.yc-mk8s.enabled", value = "true")
public class YcMk8s implements VmPoolRegistry {
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
        NodeGroup proto
    ) {}

    private record ClusterDesc(
        String clusterId,
        HostAndPort masterAddress,
        String masterCert,
        Map<String, NodeGroupDesc> nodeGroups
    ) {}

    private final Map<String, ClusterDesc> clusters = new HashMap<>();


    public YcMk8s(ServiceConfig config, ServiceFactory serviceFactory) {
        this.config = config;

        this.clusterServiceClient = serviceFactory.create(ClusterServiceBlockingStub.class, ClusterServiceGrpc::newBlockingStub)
            .withInterceptors(
                // TODO: forward X-REQUEST-ID header
                new RequestIdInterceptor());

        this.nodeGroupServiceClient = serviceFactory.create(NodeGroupServiceBlockingStub.class, NodeGroupServiceGrpc::newBlockingStub)
            .withInterceptors(
                // TODO: forward X-REQUEST-ID header
                new RequestIdInterceptor());

        config.serviceClusters().forEach(clusterId -> resolveCluster(clusterId, /* system */ true));
        config.userClusters().forEach(clusterId -> resolveCluster(clusterId, /* system */ false));
    }


    @Override
    public Map<String, VmPoolSpec> getSystemVmPools() {
        return systemPools;
    }

    @Override
    public Map<String, VmPoolSpec> getUserVmPools() {
        return userPools;
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

        if (cluster.getHealth() != Cluster.Health.UNHEALTHY) {
            var msg = "Configuration error, %s cluster %s is not healthy".formatted(ct(system), clusterId);
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        var master = cluster.getMaster();
        assert master.hasZonalMaster();
        var zonalMaster = master.getZonalMaster();
        var masterCert = master.getMasterAuth().getClusterCaCertificate();

        LOG.info("""
            Resolved {} cluster {}:
              id: {}
              folder_id: {}
              name: {}
              description: {}
              zone: {}
              k8s-master: {}
              k83-master-cert: {}
            """,
            ct(system), clusterId, cluster.getId(), cluster.getFolderId(), cluster.getName(), cluster.getDescription(),
            zonalMaster.getZoneId(), zonalMaster.getInternalV4Address(), /* masterCert */ "***");

        folder2clusters.computeIfAbsent(cluster.getFolderId(), x -> new HashSet<>()).add(clusterId);
        (system ? systemFolders : userFolders).add(cluster.getFolderId());

        var clusterDesc = new ClusterDesc(
            clusterId,
            HostAndPort.fromString(zonalMaster.getInternalV4Address()),
            masterCert,
            new HashMap<>());
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
            if (nodeGroup.getStatus() != NodeGroup.Status.RUNNING) {
                LOG.info("Skip node group {} ({}) in state {}",
                    nodeGroup.getId(), nodeGroup.getName(), nodeGroup.getStatus());
                continue;
            }

            var label = Objects.requireNonNull(nodeGroup.getLabelsMap().get("lzy.ai/node-pool-label"));

            var nodeTemplate = nodeGroup.getNodeTemplate();
            var spec = nodeTemplate.getResourcesSpec();

            var parts = nodeTemplate.getPlatformId().split(" with ", 2);
            var cpuType = parts[0];
            var gpuType = parts.length > 1 ? parts[1] : "<none>";

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
                nodeGroup.getId(), nodeGroup.getName(), cluster.getFolderId(), clusterId, zonalMaster.getZoneId(),
                label, nodeTemplate.getPlatformId(), spec.getCores(), cpuType, spec.getGpus(), gpuType,
                spec.getMemory());

            var nodeGroupDesc = new NodeGroupDesc(zonalMaster.getZoneId(), nodeGroup);
            clusterDesc.nodeGroups().put(nodeGroup.getId(), nodeGroupDesc);

            var pool = system ? systemPools : userPools;
            var vmSpec = pool.get(label);
            if (vmSpec == null) {
                vmSpec = new VmPoolSpec(label, cpuType, (int) spec.getCores(), gpuType, (int) spec.getGpus(),
                    (int) (spec.getMemory() >> 30), new HashSet<>());
                pool.put(label, vmSpec);
            }
            vmSpec.zones().add(zonalMaster.getZoneId());
        }
    }

    private static String ct(boolean system) {
        return system ? "system" : "user";
    }
}
