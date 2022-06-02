package ru.yandex.cloud.ml.platform.lzy.server.kuber.allocators;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocatorBase;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.KuberUtils;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.PodProviderException;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.ServantPodProvider;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@Requires(property = "server.kuberAllocator.enabled", value = "true")
public class KuberServantsAllocator extends ServantsAllocatorBase {
    private static final Logger LOG = LogManager.getLogger(KuberServantsAllocator.class);
    private static final String NAMESPACE = "default";

    private final ServantPodProvider provider;
    private final ConcurrentHashMap<String, V1Pod> servantPods = new ConcurrentHashMap<>();
    private final CoreV1Api api;

    public KuberServantsAllocator(Authenticator auth, ServantPodProvider provider) {
        super(auth, 10);
        this.provider = provider;
        try {
            Configuration.setDefaultApiClient(Config.defaultClient());
            api = new CoreV1Api();
        } catch (IOException e) {
            throw new RuntimeException("Cannot init KuberServantsAllocator: ", e);
        }
    }

    @Override
    protected void requestAllocation(String servantId, String servantToken, Provisioning provisioning, String bucket) {
        final V1Pod servantPodSpec;
        try {
            servantPodSpec = provider.createServantPod(
                    provisioning, servantToken, servantId, bucket
            );
        } catch (PodProviderException e) {
            throw new RuntimeException("Exception while creating servant pod spec", e);
        }
        final V1Pod pod;
        try {
            pod = api.createNamespacedPod(NAMESPACE, servantPodSpec, null, null, null, null);
        } catch (ApiException e) {
            throw new RuntimeException(String.format(
                "Exception while creating pod in kuber "
                    + "exception=%s, message=%s, errorCode=%d, responseBody=%s, stackTrace=%s",
                e,
                e.getMessage(),
                e.getCode(),
                e.getResponseBody(),
                Arrays.stream(e.getStackTrace())
                    .map(StackTraceElement::toString)
                    .collect(Collectors.joining(","))
            ));
        }
        LOG.info("Created servant pod in Kuber: {}", pod);
        Objects.requireNonNull(pod.getMetadata());
        servantPods.put(servantId, pod);
    }

    @Override
    protected void cleanup(ServantConnection s) {
        V1Pod pod = servantPods.get(s.id());
        String name = Objects.requireNonNull(pod.getMetadata()).getName();
        String namespace = pod.getMetadata().getNamespace();
        try {
            if (!isPodExists(namespace, name)) {
                servantPods.remove(s.id());
                return;
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
        terminate(s);
    }

    @Override
    protected void terminate(ServantConnection connection) {
        V1Pod pod = servantPods.get(connection.id());
        String name = Objects.requireNonNull(pod.getMetadata()).getName();
        String namespace = pod.getMetadata().getNamespace();
        try {
            if (isPodExists(namespace, name)) {
                api.deleteNamespacedPod(name, namespace, null, null, 0,
                        null, null, null);
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
        servantPods.remove(connection.id());
    }

    private boolean isPodExists(String namespace, String name) throws ApiException {
        final V1PodList listNamespacedPod = api.listNamespacedPod(
                namespace,
                null,
                null,
                null,
                null,
                "app=lzy-servant",
                Integer.MAX_VALUE,
                null,
                null,
                null,
                Boolean.FALSE
        );
        final Optional<V1Pod> queriedPod = KuberUtils.findPodByName(listNamespacedPod, name);
        return queriedPod.isPresent();
    }
}
