package ru.yandex.cloud.ml.platform.lzy.server.kuber.task;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.KuberUtils;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.PodProviderException;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.ServantPodProvider;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.ServantPodProviderImpl;
import ru.yandex.cloud.ml.platform.lzy.server.task.BaseTask;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class KuberTask extends BaseTask {
    private static final Logger LOG = LoggerFactory.getLogger(KuberTask.class);

    private final ServantPodProvider servantPodProvider = new ServantPodProviderImpl();

    public KuberTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments,
                     SnapshotMeta meta, ChannelsManager channels, URI serverURI) {
        super(owner, tid, workload, assignments, meta, channels, serverURI);
    }

    @Override
    public void start(String token) {
        LOG.info("KuberTask::start {}", token);
        try {
            V1Pod servantPodSpec = servantPodProvider.createServantPod(
                workload(), token, tid, serverURI, owner
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
                final Optional<V1Pod> queriedPod = KuberUtils.findPodByName(listNamespacedPod, podName);
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
        } catch (PodProviderException e) {
            LOG.error("KuberTask:: Exception while creating servant pod spec");
        } finally {
            LOG.info("Destroying kuber task");
            state(State.DESTROYED);
        }
    }
}
