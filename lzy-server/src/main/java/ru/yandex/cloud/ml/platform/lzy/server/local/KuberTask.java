package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardMeta;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class KuberTask extends BaseTask {
    private static final Logger LOG = LoggerFactory.getLogger(KuberTask.class);
    public static final String LZY_SERVANT_POD_TEMPLATE_FILE_PROPERTY = "lzy.servant.pod.template.file";
    public static final String DEFAULT_LZY_SERVANT_POD_TEMPLATE_FILE = "/app/resources/kubernetes/lzy-servant-pod-template.yaml";

    KuberTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments,
              WhiteboardMeta meta, ChannelsManager channels, URI serverURI) {
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
            final V1Pod servantPodDescription = (V1Pod) Yaml.load(file);
            Objects.requireNonNull(servantPodDescription.getSpec());
            Objects.requireNonNull(servantPodDescription.getMetadata());

            servantPodDescription.getSpec().getContainers().get(0).addEnvItem(
                new V1EnvVar().name("LZYTASK").value(tid.toString())
            ).addEnvItem(
                new V1EnvVar().name("LZYTOKEN").value(token)
            ).addEnvItem(
                new V1EnvVar().name("LZY_SERVER_URI").value(serverURI.toString())
            );
            // TODO: add env variables for whiteboard
            final String podName = "lzy-servant-" + tid.toString().toLowerCase(Locale.ROOT);
            servantPodDescription.getMetadata().setName(podName);

            //TODO: run on GPU node if zygote requires GPU
            // (((AtomicZygote)workload()).provisioning().tags().anyMatch(tag -> tag.tag().contains("GPU"));)

            final CoreV1Api api = new CoreV1Api();
            final String namespace = "default";
            final V1Pod pod = api.createNamespacedPod(namespace, servantPodDescription, null, null, null);
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
                final Optional<V1Pod> queriedPod =
                    listNamespacedPod
                        .getItems()
                        .stream()
                        .filter(v1pod -> podName.equals(Objects.requireNonNull(v1pod.getMetadata()).getName()))
                        .collect(Collectors.toList())
                        .stream()
                        .findFirst();
                if (queriedPod.isEmpty()) {
                    LOG.error("Not found pod " + podName);
                    break;
                }
                if (queriedPod.get().getStatus() == null || Objects.requireNonNull(queriedPod.get().getStatus()).getPhase() == null) {
                    continue;
                }
                final String phase = Objects.requireNonNull(queriedPod.get().getStatus()).getPhase();
                LOG.info("KuberTask current phase: " + phase);
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
}
