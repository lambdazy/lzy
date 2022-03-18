package ru.yandex.cloud.ml.platform.lzy.server.kuber;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1ResourceRequirementsBuilder;
import io.kubernetes.client.openapi.models.V1Toleration;
import io.kubernetes.client.openapi.models.V1TolerationBuilder;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Yaml;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;

public class ServantPodProviderImpl implements ServantPodProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ServantPodProviderImpl.class);
    private static final String LZY_SERVANT_POD_TEMPLATE_FILE_PROPERTY = "lzy.servant.pod.template.file";
    private static final String DEFAULT_LZY_SERVANT_POD_TEMPLATE_FILE =
        "/app/resources/kubernetes/lzy-servant-pod-template.yaml";
    private static final String LZY_SERVANT_CONTAINER_NAME = "lzy-servant";
    private static final V1Toleration GPU_SERVANT_POD_TOLERATION = new V1TolerationBuilder()
        .withKey("sku")
        .withOperator("Equal")
        .withValue("gpu")
        .withEffect("NoSchedule")
        .build();
    private static final List<V1Toleration> GPU_SERVANT_POD_TOLERATIONS = List.of(
        GPU_SERVANT_POD_TOLERATION
    );
    private static final V1ResourceRequirements GPU_SERVANT_POD_RESOURCE =
        new V1ResourceRequirementsBuilder().addToLimits("nvidia.com/gpu", Quantity.fromString("1")).build();

    private static boolean isNeedGpu(Zygote workload) {
        return ((AtomicZygote) workload).provisioning().tags().anyMatch(tag -> tag.tag().contains("GPU"));
    }

    @Override
    public V1Pod createServantPod(Zygote workload, String token, UUID tid, URI serverURI, String uid, String bucket)
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
        addEnvVars(container, token, tid, serverURI, bucket);

        final String podName = "lzy-servant-" + tid.toString().toLowerCase(Locale.ROOT);
        pod.getMetadata().setName(podName);

        final V1PodSpec podSpec = pod.getSpec();
        Objects.requireNonNull(podSpec);
        Objects.requireNonNull(pod.getMetadata());
        final String typeLabelValue;
        if (isNeedGpu(workload)) {
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

    private void addEnvVars(V1Container container, String token, UUID tid, URI serverURI, String bucketName) {
        container.addEnvItem(
            new V1EnvVar().name("LZYTASK").value(tid.toString())
        ).addEnvItem(
            new V1EnvVar().name("LZYTOKEN").value(token)
        ).addEnvItem(
            new V1EnvVar().name("LZY_SERVER_URI").value(serverURI.toString())
        ).addEnvItem(
            new V1EnvVar().name("LZYWHITEBOARD").value(System.getenv("SERVER_WHITEBOARD_URL"))
        ).addEnvItem(
            new V1EnvVar().name("BUCKET_NAME").value(bucketName)
        ).addEnvItem(
            new V1EnvVar().name("BASE_ENV_DEFAULT_IMAGE").value(System.getenv("BASE_ENV_DEFAULT_IMAGE"))
        );
    }
}
