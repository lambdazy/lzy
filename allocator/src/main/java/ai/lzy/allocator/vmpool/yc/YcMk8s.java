package ai.lzy.allocator.vmpool.yc;

import ai.lzy.allocator.configs.ServiceConfig;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.annotation.Requires;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.k8s.v1.ClusterServiceGrpc;
import yandex.cloud.api.k8s.v1.ClusterServiceOuterClass.GetClusterRequest;
import yandex.cloud.api.k8s.v1.NodeGroupOuterClass.NodeGroup;
import yandex.cloud.api.k8s.v1.NodeGroupServiceGrpc;
import yandex.cloud.api.k8s.v1.NodeGroupServiceOuterClass;
import yandex.cloud.api.k8s.v1.NodeGroupServiceOuterClass.ListNodeGroupsRequest;
import yandex.cloud.sdk.ChannelFactory;
import yandex.cloud.sdk.grpc.interceptors.DeadlineClientInterceptor;
import yandex.cloud.sdk.grpc.interceptors.RequestIdInterceptor;

import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.Objects;

import static yandex.cloud.api.k8s.v1.ClusterOuterClass.Cluster;

@Singleton
@Requires(property = "allocator.yc-mk8s.enabled", value = "true")
public class YcMk8s {
    private static final Logger LOG = LogManager.getLogger(YcMk8s.class);

    private static final Duration YC_CALL_TIMEOUT = Duration.ofSeconds(30);

    private final ServiceConfig config;
    private final ManagedChannel clusterServiceChannel;
    private final ManagedChannel nodeGroupServiceChannel;
    private final ClusterServiceGrpc.ClusterServiceBlockingStub clusterServiceClient;
    private final NodeGroupServiceGrpc.NodeGroupServiceBlockingStub nodeGroupServiceClient;


    public YcMk8s(ServiceConfig config) {
        this.config = config;

        // TODO: retries
        var cf = ChannelFactory.getDefaultChannelFactory();

        this.clusterServiceChannel = cf.getChannel(ClusterServiceGrpc.ClusterServiceBlockingStub.class);
        this.nodeGroupServiceChannel = cf.getChannel(NodeGroupServiceGrpc.NodeGroupServiceBlockingStub.class);

        this.clusterServiceClient = ClusterServiceGrpc.newBlockingStub(clusterServiceChannel)
            .withInterceptors(new RequestIdInterceptor(), DeadlineClientInterceptor.fromDuration(YC_CALL_TIMEOUT));

        this.nodeGroupServiceClient = NodeGroupServiceGrpc.newBlockingStub(nodeGroupServiceChannel)
            .withInterceptors(new RequestIdInterceptor(), DeadlineClientInterceptor.fromDuration(YC_CALL_TIMEOUT));

        config.serviceClusters().forEach(clusterId -> resolveCluster(clusterId, /* system */ true));
        config.userClusters().forEach(clusterId -> resolveCluster(clusterId, /* system */ false));
    }

    @PreDestroy
    public void shutdown() {
        clusterServiceChannel.shutdown();
        nodeGroupServiceChannel.shutdown();
    }

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
                label, nodeTemplate.getPlatformId(), spec.getCores(), parts[0],
                spec.getGpus(), parts.length > 1 ? parts[1] : "<none>", spec.getMemory());
        }
    }

    private static String ct(boolean system) {
        return system ? "system" : "user";
    }
}
