package ru.yandex.cloud.ml.platform.lzy.server.kuber.task;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.ProvisioningPodFactory;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.ProvisioningPodFactoryImpl;
import ru.yandex.cloud.ml.platform.lzy.server.task.BaseTask;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class KuberTask extends BaseTask {
    private static final Logger LOG = LoggerFactory.getLogger(KuberTask.class);

    private final ProvisioningPodFactory provisioningPodFactory = new ProvisioningPodFactoryImpl();

    public KuberTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments,
                     SnapshotMeta meta, ChannelsManager channels, URI serverURI) {
        super(owner, tid, workload, assignments, meta, channels, serverURI);
    }

    @Override
    public void start(String token) {
        LOG.info("KuberTask::start {}", token);
        try {
            V1Pod servantPodSpec = provisioningPodFactory.fillPodSpecWithProvisioning(
                workload(), token, tid, serverURI
            );
            final CoreV1Api api = new CoreV1Api();
            final String namespace = "default";
            final V1Pod pod = api.createNamespacedPod(namespace, servantPodSpec, null, null, null);
            Objects.requireNonNull(pod.getMetadata());

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
                String podName = pod.getMetadata().getName();
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
        } catch (InterruptedException e) {
            throw new RuntimeException("KuberTask:: Exception while execution. " + e);
        } catch (ApiException e) {
            LOG.error("KuberTask:: API exception while pod creation. " + e);
            LOG.error(e.getResponseBody());
        } finally {
            LOG.info("Destroying kuber task");
            state(State.DESTROYED);
        }
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
