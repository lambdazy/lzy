package ai.lzy.allocator.admin;

import ai.lzy.allocator.admin.dao.AdminDao;
import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.ActiveImages;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import com.amazonaws.util.StringInputStream;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
@Requires(property = "allocator.internal-image-updater.enabled", value = "true")
public class ImagesUpdater {
    private static final Logger LOG = LogManager.getLogger(ImagesUpdater.class);

    private static final String FICTIVE_NS = "fictive";
    private static final String DAEMONSET_TEMPLATE = "kubernetes/lzy-fictive-daemonset-template.yaml.ftl";

    private final String address;
    private final KuberClientFactory kuberClientFactory;
    private final ClusterRegistry clusterRegistry;
    private final Template daemonSetTemplate;

    public ImagesUpdater(ServiceConfig serviceConfig, KuberClientFactory kuberClientFactory,
                         ClusterRegistry clusterRegistry, AdminDao adminDao) throws Exception
    {
        this.address = serviceConfig.getAddress();
        this.kuberClientFactory = kuberClientFactory;
        this.clusterRegistry = clusterRegistry;

        var ftlConfig = new Configuration(Configuration.VERSION_2_3_22);
        ftlConfig.setClassLoaderForTemplateLoading(getClass().getClassLoader(), "/");
        this.daemonSetTemplate = ftlConfig.getTemplate(DAEMONSET_TEMPLATE);

        var hasDaemonSets = printFictiveDaemonSets(kuberClientFactory, clusterRegistry);
        var hasDbConfig = !adminDao.getImages().isEmpty();

        if (hasDaemonSets && !hasDbConfig) {
            throw new RuntimeException("Invalid configuration: has active daemonsets but no configuration in DB");
        }
    }

    public void update(ActiveImages.Configuration conf) throws UpdateDaemonSetsException {
        for (var cluster : clusterRegistry.listClusters(ClusterRegistry.ClusterType.User)) {
            updateCluster(cluster, conf);
        }
    }

    private void updateCluster(ClusterRegistry.ClusterDescription cluster, ActiveImages.Configuration configuration)
        throws UpdateDaemonSetsException
    {
        try (var kuberClient = kuberClientFactory.build(cluster)) {
            createFictiveNamespace(kuberClient, cluster.clusterId());

            for (var pool : cluster.pools().entrySet()) {
                createDaemonSetForCluster(cluster, configuration, kuberClient, pool.getKey(), pool.getValue());
            }
        } catch (Exception e) {
            LOG.error("Cannot update cluster {}: {}", cluster.clusterId(), e.getMessage());
            throw new UpdateDaemonSetsException(e.getMessage());
        }
    }

    private void createDaemonSetForCluster(ClusterRegistry.ClusterDescription cluster,
                                           ActiveImages.Configuration configuration,
                                           KubernetesClient kuberClient,
                                           String poolName, ClusterRegistry.PoolType poolType)
        throws TemplateException, IOException
    {
        var args = new HashMap<String, Object>();
        args.put("pool", Map.of(
            "name", poolName,
            "kind", poolType.name().toUpperCase()
            ));

        var workers = configuration.workers();
        args.put("workers", new ArrayList<>());
        for (int i = 0; i < workers.size(); i++) {
            var worker = workers.get(i);
            ((List<Object>) args.get("workers")).add(Map.of(
                "name", poolName + "-" + i,
                "image", worker.image()
            ));
        }

        var jupyterLabs = configuration.jupyterLabs();
        args.put("jls", new ArrayList<>());
        for (int i = 0; i < jupyterLabs.size(); i++) {
            var jl = jupyterLabs.get(i);
            ((List<Object>) args.get("jls")).add(Map.of(
                "name", poolName + "-" + i,
                "main_image", jl.mainImage(),
                "additional_images", Arrays.asList(jl.additionalImages())
            ));
        }

        args.put("allocator_sync_image", configuration.sync().image());

        args.put("cluster_id", cluster.clusterId());
        args.put("allocator_ip", address);

        var buf = new StringWriter();
        daemonSetTemplate.process(args, buf);
        var specStr = buf.toString();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Attempt to create daemonset:\n{}", specStr);
        } else {
            LOG.info("Attempt to create daemonset with args: {}", args);
        }

        var spec = kuberClient.apps().daemonSets()
            .load(new StringInputStream(specStr))
            .get();

        try {
            kuberClient.apps().daemonSets()
                .inNamespace(FICTIVE_NS)
                .resource(spec)
                .createOrReplace();
            LOG.info("Daemonset {} successfully created", args);
        } catch (KubernetesClientException e) {
            LOG.error("Cannot create daemonset with args {}: {}", args, e.getMessage());
            throw e;
        }
    }

    private boolean hasFictiveNamespace(KubernetesClient client, String clusterId) throws UpdateDaemonSetsException {
        try {
            client.namespaces().withName(FICTIVE_NS).get();
            return true;
        } catch (Exception e) {
            if (KuberUtils.isResourceNotFound(e)) {
                return false;
            }
            LOG.error("Cluster {} failure: {}", clusterId, e.getMessage(), e);
            throw new UpdateDaemonSetsException(e.getMessage());
        }
    }

    private void createFictiveNamespace(KubernetesClient client, String clusterId) throws UpdateDaemonSetsException {
        if (hasFictiveNamespace(client, clusterId)) {
            return;
        }

        try {
            client.namespaces()
                .resource(new NamespaceBuilder()
                    .withMetadata(new ObjectMetaBuilder()
                        .withName(FICTIVE_NS)
                        .build())
                    .build())
                .create();
        } catch (Exception e) {
            if (KuberUtils.isResourceAlreadyExist(e)) {
                if (hasFictiveNamespace(client, clusterId)) {
                    return;
                }
                throw new UpdateDaemonSetsException("Cluster %s, cannot create namespace %s: %s"
                    .formatted(clusterId, FICTIVE_NS, e.getMessage()));
            }
            LOG.error("Cluster {} failure, cannot create {} namespace: {}", clusterId, FICTIVE_NS, e.getMessage(), e);
            throw new UpdateDaemonSetsException(e.getMessage());
        }
    }

    private static boolean printFictiveDaemonSets(KuberClientFactory kuberClientFactory,
                                                  ClusterRegistry clusterRegistry)
    {
        boolean hasDaemonSets = false;
        for (var cluster : clusterRegistry.listClusters(ClusterRegistry.ClusterType.User)) {
            try (var client = kuberClientFactory.build(cluster)) {
                try {
                    var daemonsets = client.apps().daemonSets()
                        .inNamespace(FICTIVE_NS)
                        .list();

                    for (var ds : daemonsets.getItems()) {
                        var sb = new StringBuilder();
                        sb.append("User cluster '").append(cluster.clusterId()).append("' has fictive DaemonSet:\n");
                        sb.append("  name            : ").append(ds.getMetadata().getName()).append('\n');
                        sb.append("  created at      : ").append(ds.getMetadata().getCreationTimestamp()).append('\n');
                        sb.append("  ready           : ").append(ds.getStatus().getNumberReady()).append('\n');
                        sb.append("  available       : ").append(ds.getStatus().getNumberAvailable()).append('\n');
                        sb.append("  selector        : ").append('\n');
                        for (var selector : ds.getSpec().getTemplate().getSpec().getNodeSelector().entrySet()) {
                            sb.append("    ").append(selector.getKey()).append(" : ")
                                .append(selector.getValue()).append('\n');
                        }
                        sb.append("  init containers :").append('\n');
                        for (var ic : ds.getSpec().getTemplate().getSpec().getInitContainers()) {
                            sb.append("    * ").append(ic.getName()).append('\n');
                            sb.append("      image: ").append(ic.getImage()).append('\n');
                            sb.append("      cmd  : ").append(ic.getCommand()).append('\n');
                        }
                        for (var c : ds.getSpec().getTemplate().getSpec().getContainers()) {
                            sb.append("    * ").append(c.getName()).append('\n');
                            sb.append("      image: ").append(c.getImage()).append('\n');
                            sb.append("      cmd  : ").append(c.getCommand()).append('\n');
                        }

                        LOG.info(sb);
                        hasDaemonSets = true;
                    }
                } catch (KubernetesClientException e) {
                    LOG.error("Cannot list daemonsets in the {} namespace: {}", FICTIVE_NS, e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
        return hasDaemonSets;
    }

    public static final class UpdateDaemonSetsException extends Exception {
        public UpdateDaemonSetsException(String message) {
            super(message);
        }
    }
}
