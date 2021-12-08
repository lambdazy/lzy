package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Yaml;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.qe.s3.util.Environment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class KuberTask extends BaseTask {
    private static final Logger LOG = LoggerFactory.getLogger(KuberTask.class);
    private static final String LZY_SERVANT_POD_TEMPLATE_FILE_PROPERTY = "lzy.servant.pod.template.file";
    private static final String DEFAULT_LZY_SERVANT_POD_TEMPLATE_FILE = "/app/resources/kubernetes/lzy-servant-pod-template.yaml";
    private static final String LZY_SERVANT_CONTAINER_NAME = "lzy-servant";

    private final ProvisioningPodFactory provisioningPodFactory = new ProvisioningPodFactoryImpl();

    KuberTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments,
              SnapshotMeta meta, ChannelsManager channels, URI serverURI) {
        super(owner, tid, workload, assignments, meta, channels, serverURI);
    }

    @Override
    public void start(String token) {
        LOG.info("KuberTask::start {}", token);
        try {
            final ApiClient client = ClientBuilder.cluster().build();
            Configuration.setDefaultApiClient(client);

            final String lzyServantPodTemplatePath = System.getProperty(
                    LZY_SERVANT_POD_TEMPLATE_FILE_PROPERTY,
                    DEFAULT_LZY_SERVANT_POD_TEMPLATE_FILE
            );
            final File file = new File(lzyServantPodTemplatePath);
            V1Pod servantPod = (V1Pod) Yaml.load(file);
            Objects.requireNonNull(servantPod.getSpec());
            Objects.requireNonNull(servantPod.getMetadata());

            final Optional<V1Container> containerOptional = findContainerByName(servantPod, LZY_SERVANT_CONTAINER_NAME);
            if (containerOptional.isEmpty()) {
                LOG.error("lzy servant pod spec doesn't contain {} container", LZY_SERVANT_CONTAINER_NAME);
                return;
            }
            final V1Container container = containerOptional.get();
            addEnvVars(container, token);

            final String podName = "lzy-servant-" + tid.toString().toLowerCase(Locale.ROOT);
            servantPod.getMetadata().setName(podName);

            servantPod = provisioningPodFactory.fillPodSpecWithProvisioning(servantPod, workload());

            final CoreV1Api api = new CoreV1Api();
            final String namespace = "default";
            final V1Pod pod = api.createNamespacedPod(namespace, servantPod, null, null, null);
            LOG.info("Created servant pod in Kuber: {}", pod);
            while (true) {
                //noinspection BusyWait
                Thread.sleep(2000); // sleep for 2 second
                final V1PodList listNamespacedPod = api.listNamespacedPod(
                    "default",
                    null,
                    null,
                    null,
                    null,
                    "app=lzy-servant",
                    Integer.MAX_VALUE,
                    null,
                    null,
                    Boolean.FALSE
                );
                final Optional<V1Pod> queriedPod = findPodByName(listNamespacedPod, podName);
                if (queriedPod.isEmpty()) {
                    LOG.error("Not found pod " + podName);
                    break;
                }
                if (queriedPod.get().getStatus() == null || Objects.requireNonNull(queriedPod.get().getStatus()).getPhase() == null) {
                    continue;
                }
                final String phase = Objects.requireNonNull(queriedPod.get().getStatus()).getPhase();
                LOG.info("KuberTask current phase: " + phase);
                // TODO: handle "Failed" phase
                if ("Succeeded".equals(phase) || "Failed".equals(phase)) {
                    api.deleteNamespacedPod(podName, namespace, null, null, null, null, null, null);
                    break;
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("KuberTask:: Exception while execution. " + e);
        } catch (ApiException e) {
            LOG.error("KuberTask:: API exception while pod creation. " + e);
            LOG.error(e.getResponseBody());
        } finally {
            LOG.info("Destroying kuber task");
            state(State.DESTROYED);
        }
    }

    private void addEnvVars(V1Container container, String token) {
        container.addEnvItem(
                new V1EnvVar().name("LZYTASK").value(tid.toString())
        ).addEnvItem(
                new V1EnvVar().name("LZYTOKEN").value(token)
        ).addEnvItem(
                new V1EnvVar().name("LZY_SERVER_URI").value(serverURI.toString())
        ).addEnvItem(
                new V1EnvVar().name("BUCKET_NAME").value(Environment.getBucketName())
        ).addEnvItem(
                new V1EnvVar().name("ACCESS_KEY").value(Environment.getAccessKey())
        ).addEnvItem(
                new V1EnvVar().name("SECRET_KEY").value(Environment.getSecretKey())
        ).addEnvItem(
                new V1EnvVar().name("REGION").value(Environment.getRegion())
        ).addEnvItem(
                new V1EnvVar().name("SERVICE_ENDPOINT").value(Environment.getServiceEndpoint())
        ).addEnvItem(
                new V1EnvVar().name("PATH_STYLE_ACCESS_ENABLED").value(Environment.getPathStyleAccessEnabled())
        ).addEnvItem(
                new V1EnvVar().name("LZYWHITEBOARD").value(Environment.getLzyWhiteboard())
        );
    }

    @NotNull
    private static Optional<V1Container> findContainerByName(V1Pod servantPodDescription, String name) {
        return Objects.requireNonNull(servantPodDescription.getSpec())
                .getContainers()
                .stream()
                .filter(c -> name.equals(c.getName()))
                .findFirst();
    }

    @NotNull
    private static Optional<V1Pod> findPodByName(V1PodList listNamespacedPod, String podName) {
        return listNamespacedPod
            .getItems()
            .stream()
            .filter(v1pod -> podName.equals(Objects.requireNonNull(v1pod.getMetadata()).getName()))
            .collect(Collectors.toList())
            .stream()
            .findFirst();
    }
}
