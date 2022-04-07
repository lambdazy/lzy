package ru.yandex.cloud.ml.platform.lzy.server.kuber;
//
//import io.kubernetes.client.openapi.ApiException;
//import io.kubernetes.client.openapi.apis.CoreV1Api;
//import io.kubernetes.client.openapi.models.V1Pod;
//import io.kubernetes.client.openapi.models.V1PodList;
//import java.net.URI;
//import java.util.Map;
//import java.util.Objects;
//import java.util.Optional;
//import java.util.UUID;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import ru.yandex.cloud.ml.platform.lzy.model.Slot;
//import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
//import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
//import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
//import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
//import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocator;
//
//public class KuberServantAllocator implements ServantsAllocator {
//    private static final Logger LOG = LoggerFactory.getLogger(KuberServantAllocator.class);
//
//    private final ServantPodProvider servantPodProvider = new ServantPodProviderImpl();
//
//    public KuberServantAllocator(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments,
//                                 ChannelsManager channels, URI serverURI, String bucket) {
//        super(owner, tid, workload, assignments, channels, serverURI, bucket);
//    }
//
//    @Override
//    public void start(String token) {
//        LOG.info("KuberTask::start {}", token);
//        try {
//            V1Pod servantPodSpec = servantPodProvider.createServantPod(
//                workload(), token, tid, serverURI, owner, bucket()
//            );
//            final CoreV1Api api = new CoreV1Api();
//            final String namespace = "default";
//            final long sendTaskMillis = System.currentTimeMillis();
//            final V1Pod pod = api.createNamespacedPod(namespace, servantPodSpec, null, null, null);
//            LOG.info("Created servant pod in Kuber: {}", pod);
//            Objects.requireNonNull(pod.getMetadata());
//
//            boolean metricLogged = false;
//            String prevPhase = null;
//            while (true) {
//                //noinspection BusyWait
//                Thread.sleep(2000); // sleep for 2 second
//                final V1PodList listNamespacedPod = api.listNamespacedPod(
//                    "default",
//                    null,
//                    null,
//                    null,
//                    null,
//                    "app=lzy-servant",
//                    Integer.MAX_VALUE,
//                    null,
//                     null,
//                    Boolean.FALSE
//                );
//                String podName = pod.getMetadata().getName();
//                final Optional<V1Pod> queriedPod = KuberUtils.findPodByName(listNamespacedPod, podName);
//                if (queriedPod.isEmpty()) {
//                    LOG.error("Not found pod " + podName);
//                    break;
//                }
//                if (queriedPod.get().getStatus() == null
//                    || Objects.requireNonNull(queriedPod.get().getStatus()).getPhase() == null) {
//                    continue;
//                }
//                final String phase = Objects.requireNonNull(queriedPod.get().getStatus()).getPhase();
//                if (prevPhase == null || !prevPhase.equals(phase)) {
//                    LOG.info("KuberTask:: {} pod current phase: {}", pod.getMetadata().getName(), phase);
//                    prevPhase = phase;
//                }
//                // TODO: handle "Failed" phase
//                if ("Succeeded".equals(phase) || "Failed".equals(phase)) {
//                    LOG.info("KuberTask:: Servant exited with phase {}", phase);
//                    api.deleteNamespacedPod(podName, namespace, null, null, null, null, null, null);
//                    break;
//                } else if ("Running".equals(phase)) {
//                    // It's necessary to log metric only 1 time
//                    if (!metricLogged) {
//                        final long taskRunningMillis = System.currentTimeMillis();
//                        MetricEventLogger.log(
//                            new MetricEvent(
//                                "time from send KuberTask to Servant Running status",
//                                Map.of(),
//                                taskRunningMillis - sendTaskMillis
//                            )
//                        );
//                        metricLogged = true;
//                    }
//                }
//            }
//        } catch (InterruptedException e) {
//            throw new RuntimeException("KuberTask:: Exception while execution. " + e);
//        } catch (ApiException e) {
//            throw new RuntimeException("KuberTask:: API exception while kubernetes pod creation", e);
//        } catch (PodProviderException e) {
//            throw new RuntimeException("KuberTask:: Exception while creating servant pod spec", e);
//        } finally {
//            LOG.info("Destroying kuber task");
//            state(State.DESTROYED);
//        }
//    }
//}

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocatorBase;

import java.util.Objects;

public class KuberServantAllocator extends ServantsAllocatorBase {
    private final ServantPodProvider provider;
    private final static Logger LOG = LogManager.getLogger(KuberServantAllocator.class);

    public KuberServantAllocator(Authenticator auth, ServantPodProvider provider) {
        super(auth, 10);
        this.provider = provider;
    }

    @Override
    protected void requestAllocation(String servantId, String servantToken, Provisioning provisioning,
                                     Env env, String bucket) {
        final V1Pod servantPodSpec;
        try {
            servantPodSpec = provider.createServantPod(
                provisioning, servantToken, servantId, bucket
            );
        } catch (PodProviderException e) {
            throw new RuntimeException("Exception while creating servant pod spec", e);
        }

        final CoreV1Api api = new CoreV1Api();
        final String namespace = "default";
        final long sendTaskMillis = System.currentTimeMillis();
        final V1Pod pod;
        try {
            pod = api.createNamespacedPod(namespace, servantPodSpec, null, null, null);
        } catch (ApiException e) {
            throw new RuntimeException("Exception while creating pod in kuber", e);
        }
        LOG.info("Created servant pod in Kuber: {}", pod);
        Objects.requireNonNull(pod.getMetadata());


    }

    @Override
    protected void cleanup(ServantConnection s) {

    }

    @Override
    protected void terminate(ServantConnection connection) {

    }
}
