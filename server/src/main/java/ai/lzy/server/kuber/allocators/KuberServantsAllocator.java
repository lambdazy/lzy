package ai.lzy.server.kuber.allocators;

import ai.lzy.server.Authenticator;
import ai.lzy.server.ServantsAllocatorBase;
import ai.lzy.server.kuber.KuberUtils;
import ai.lzy.server.kuber.PodProviderException;
import ai.lzy.server.kuber.ServantPodProvider;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.graph.Provisioning;

import javax.print.attribute.standard.MediaSize;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static ai.lzy.server.kuber.KuberUtils.kuberValidName;
import static ai.lzy.server.kuber.KuberUtils.listPods;

@Singleton
@Requires(property = "server.kuberAllocator.enabled", value = "true")
public class KuberServantsAllocator extends ServantsAllocatorBase {
    private static final Logger LOG = LogManager.getLogger(KuberServantsAllocator.class);
    private static final String NAMESPACE = "default";

    private final ServantPodProvider provider;
    private final ConcurrentHashMap<String, V1Pod> servantPods = new ConcurrentHashMap<>();
    private final CoreV1Api api;

    public KuberServantsAllocator(Authenticator auth, ServantPodProvider provider) {
        super(auth, 10, 3600);
        this.provider = provider;
        try {
            Configuration.setDefaultApiClient(Config.defaultClient());
            api = new CoreV1Api();
        } catch (IOException e) {
            throw new RuntimeException("Cannot init KuberServantsAllocator: ", e);
        }
    }

    @Override
    protected void requestAllocation(
            String sessionId, String servantId, String servantToken, Provisioning provisioning, String bucket
    ) {
        // LZY SERVANT POD CREATION
        final V1Pod declaredServantPod;
        try {
            declaredServantPod = provider.createServantPod(
                provisioning, servantToken, servantId, bucket, sessionId
            );
        } catch (PodProviderException e) {
            throw new RuntimeException("Exception while creating servant pod spec", e);
        }
        V1Pod createdServantPod;
        try {
            createdServantPod = api.createNamespacedPod(NAMESPACE, declaredServantPod, null, null, null, null);
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
        LOG.info("Created servant pod in Kuber: {}", createdServantPod);
        servantPods.put(servantId, createdServantPod);
        Objects.requireNonNull(createdServantPod.getMetadata());
        String servantPodName = createdServantPod.getMetadata().getName();

        var servantPods = listPods(api, NAMESPACE, "app=lzy-servant,session-id=" + kuberValidName(sessionId));
        createdServantPod = KuberUtils.findPodByName(servantPods, servantPodName).orElseThrow(
                () -> new RuntimeException("Didn't find requested servant pod in kuber: " + servantPodName)
        );
        if ("Pending".equals(Objects.requireNonNull(createdServantPod.getStatus()).getPhase())) {
            // TODO: think about order of these requests
            servantPods = listPods(api, NAMESPACE, "app=lzy-servant,session-id=" + kuberValidName(sessionId));
            var lockPods = listPods(api, NAMESPACE, "app=servant-lock,session-id=" + kuberValidName(sessionId));
            if (lockPods.getItems().size() < servantPods.getItems().size()) {
                lockNewNodePerSession(sessionId, servantId, provisioning);
            }
a            // TODO: think also about deleting excess lock pods (shrink to fit)
        }
        // TODO: research +- optimal algorithm for locking nodes per user
    }

    private void lockNewNodePerSession(String sessionId, String servantId, Provisioning provisioning) {
        // SERVANT LOCK POD CREATION
        final V1Pod declaredServantLockPod;
        try {
            declaredServantLockPod = provider.createServantLockPod(
                    provisioning, servantId, sessionId
            );
        } catch (PodProviderException e) {
            throw new RuntimeException("Exception while creating servant lock pod spec", e);
        }
        final V1Pod createdServantLockPod;
        try {
            createdServantLockPod = api.createNamespacedPod(NAMESPACE, declaredServantLockPod, null, null, null, null);
        } catch (ApiException e) {
            throw new RuntimeException(String.format(
                "Exception while creating servant lock pod in kuber "
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
        LOG.info("Created servant lock pod for session {} in Kuber: {}", sessionId, createdServantLockPod);
    }

    @Override
    public synchronized void deleteSession(String sessionId) {
        super.deleteSession(sessionId);
        // TODO delete or drain all locked nodes
        try {
            api.deleteCollectionNamespacedPod(
                NAMESPACE, null, null, null, null, 1,
                "session-id=" + kuberValidName(sessionId), Integer.MAX_VALUE,
                null, null, null, null,
                Integer.MAX_VALUE, null
            );
        } catch (ApiException e) {
            throw new RuntimeException(
                String.format(
                    "Exception while listing servant lock pods for session %s in kuber "
                        + "exception=%s, message=%s, errorCode=%d, responseBody=%s, stackTrace=%s",
                    sessionId,
                    e,
                    e.getMessage(),
                    e.getCode(),
                    e.getResponseBody(),
                    Arrays.stream(e.getStackTrace())
                        .map(StackTraceElement::toString)
                        .collect(Collectors.joining(","))
                )
            );
        }
    }

    @Override
    protected void cleanup(ServantConnection s) {
        V1Pod pod = servantPods.get(s.id());
        if (pod == null) {
            LOG.warn("Trying to cleanup nonexistent connection {}", s.id());
            return;
        }
        String name = Objects.requireNonNull(pod.getMetadata()).getName();
        String namespace = pod.getMetadata().getNamespace();
        LOG.info("Cleaning up pod {} with connection {}", name, s.id());
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
        if (pod == null) {
            LOG.warn("Trying to terminate nonexistent connection {}", connection.id());
            return;
        }
        String name = Objects.requireNonNull(pod.getMetadata()).getName();
        String namespace = pod.getMetadata().getNamespace();
        LOG.info("Terminating up pod {} with connection {}", name, connection.id());
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
