package ai.lzy.scheduler.allocator.impl.kuber;

import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.configs.ServiceConfig;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Yaml;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Singleton
public class ServantPodProviderImpl implements ServantPodProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ServantPodProviderImpl.class);
    private static final String LZY_SERVANT_POD_TEMPLATE_FILE_PROPERTY = "lzy.servant.pod.template.file";
    private static final String DEFAULT_LZY_SERVANT_POD_TEMPLATE_FILE =
        "/app/resources/kubernetes/lzy-servant-pod-template.yaml";
    private static final String LZY_SERVANT_CONTAINER_NAME = "lzy-servant";
    private static final V1Toleration GPU_SERVANT_POD_TOLERATION = new V1Toleration()
        .key("sku")
        .operator("Equal")
        .value("gpu")
        .effect("NoSchedule");
    private static final List<V1Toleration> GPU_SERVANT_POD_TOLERATIONS = List.of(
        GPU_SERVANT_POD_TOLERATION
    );
    private static final V1ResourceRequirements GPU_SERVANT_POD_RESOURCE =
        new V1ResourceRequirements().putLimitsItem("nvidia.com/gpu", Quantity.fromString("1"));

    @Inject
    private ServiceConfig serverConfig;

    private static boolean isNeedGpu(Provisioning provisioning) {
        return provisioning.tags().stream().anyMatch(tag -> tag.contains("GPU"));
    }

    @Override
    public V1Pod createServantPod(Provisioning provisioning, String token, String servantId, String workflowId)
        throws PodProviderException {
        try {
            final ApiClient client = ClientBuilder.cluster().build();
            Configuration.setDefaultApiClient(client);
        } catch (IOException e) {
            LOG.error("IO error while finding Kubernetes config");
            throw new PodProviderException("cannot load kuber api client", e);
        }

        final V1Pod pod;
        final String lzyServantPodTemplatePath = System.getProperty(
            LZY_SERVANT_POD_TEMPLATE_FILE_PROPERTY,
            DEFAULT_LZY_SERVANT_POD_TEMPLATE_FILE
        );
        try {
            final File file = new File(lzyServantPodTemplatePath);
            pod = (V1Pod) Yaml.load(file);
        } catch (IOException e) {
            LOG.error("IO error while loading yaml file {}", lzyServantPodTemplatePath);
            throw new PodProviderException("cannot load servant yaml file", e);
        }

        Objects.requireNonNull(pod.getSpec());
        Objects.requireNonNull(pod.getMetadata());
        if (System.getenv("SERVANT_IMAGE") != null) {
            pod.getSpec().getContainers().get(0).setImage(System.getenv("SERVANT_IMAGE"));
        }

        final Optional<V1Container> containerOptional = KuberUtils.findContainerByName(pod, LZY_SERVANT_CONTAINER_NAME);
        if (containerOptional.isEmpty()) {
            LOG.error("lzy servant pod spec doesn't contain {} container", LZY_SERVANT_CONTAINER_NAME);
            throw new PodProviderException("cannot find " + LZY_SERVANT_CONTAINER_NAME + " container in pod spec");
        }
        final V1Container container = containerOptional.get();
        addEnvVars(container, token, servantId, workflowId);

        final String podName = "lzy-servant-" + servantId.toLowerCase(Locale.ROOT);
        // k8s pod name can only contain symbols [-a-z0-9]
        pod.getMetadata().setName(podName.replaceAll("[^-a-z0-9]", "-"));

        final V1PodSpec podSpec = pod.getSpec();
        Objects.requireNonNull(podSpec);
        Objects.requireNonNull(pod.getMetadata());
        final String typeLabelValue;
        if (isNeedGpu(provisioning)) {
            typeLabelValue = "gpu";
            podSpec.setTolerations(GPU_SERVANT_POD_TOLERATIONS);
            podSpec.getContainers().get(0).setResources(GPU_SERVANT_POD_RESOURCE);
        } else {
            typeLabelValue = "cpu";
        }
        final Map<String, String> nodeSelector = Map.of("type", typeLabelValue);
        podSpec.setNodeSelector(nodeSelector);

        return pod;
    }

    private void addEnvVars(V1Container container, String token,
                            String servantId, String workflowId) {
        container.addEnvItem(
            new V1EnvVar().name("SERVANT_ID").value(servantId)
        ).addEnvItem(
            new V1EnvVar().name("SERVANT_TOKEN").value(token)
        ).addEnvItem(
            new V1EnvVar().name("LZY_SERVER_URI").value("http://" + serverConfig.schedulerAddress())
        ).addEnvItem(
            new V1EnvVar().name("LZYWHITEBOARD").value("http://" + serverConfig.whiteboardAddress())
        ).addEnvItem(
            new V1EnvVar().name("BASE_ENV_DEFAULT_IMAGE").value(serverConfig.baseEnvDefaultImage())
        ).addEnvItem(
            new V1EnvVar().name("WORKFLOW_ID").value(workflowId)
        );
    }
}
