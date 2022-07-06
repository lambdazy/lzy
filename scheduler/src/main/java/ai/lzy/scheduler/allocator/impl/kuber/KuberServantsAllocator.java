package ai.lzy.scheduler.allocator.impl.kuber;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.allocator.ServantsAllocatorBase;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.servant.Servant;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.gsonfire.builders.JsonObjectBuilder;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
@Requires(property = "scheduler.kuberAllocator.enabled", value = "true")
public class KuberServantsAllocator extends ServantsAllocatorBase {
    private static final Logger LOG = LogManager.getLogger(KuberServantsAllocator.class);
    private static final String NAMESPACE = "default";

    private final ServantPodProvider provider;
    private final CoreV1Api api;

    @Inject
    public KuberServantsAllocator(ServantDao dao, ServantPodProvider provider, ServantMetaStorage metaStorage) {
        super(dao, metaStorage);
        this.provider = provider;
        try {
            Configuration.setDefaultApiClient(Config.defaultClient());
            api = new CoreV1Api();
        } catch (IOException e) {
            throw new RuntimeException("Cannot init KuberServantsAllocator: ", e);
        }
    }

    protected KuberMeta requestAllocation(String workflowId, String servantId, String servantToken,
                                          Provisioning provisioning) {
        final V1Pod servantPodSpec;
        try {
            servantPodSpec = provider.createServantPod(
                    provisioning, servantToken, servantId, workflowId
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
        return new KuberMeta(pod.getMetadata().getNamespace(), pod.getMetadata().getName());
    }

    protected void terminate(String namespace, String name) {
        try {
            if (isPodExists(namespace, name)) {
                api.deleteNamespacedPod(name, namespace, null, null, 0,
                        null, null, null);
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
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

    @Override
    public void allocate(String workflowId, String servantId, Provisioning provisioning) {
        final KuberMeta meta = requestAllocation(workflowId, servantId, "", provisioning);  // TODO(artolord) add token
        metaStorage.saveMeta(workflowId, servantId, meta.toJson());
    }

    @Override
    public void destroy(String workflowId, String servantId) throws Exception {
        var json = metaStorage.getMeta(workflowId, servantId);
        metaStorage.clear(workflowId, servantId);
        if (json == null) {
            throw new Exception("Cannot get servant from db");
        }
        final KuberMeta meta = KuberMeta.fromJson(json);
        if (meta == null) {
            throw new Exception("Cannot parse servant metadata");
        }
        terminate(meta.namespace(), meta.podName());
    }

    private record KuberMeta(String namespace, String podName) {
        String toJson() {
            return new JsonObjectBuilder()
                .set("namespace", namespace)
                .set("podName", podName)
                .build()
                .toString();
        }

        @Nullable
        static KuberMeta fromJson(String json) {
            final JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
            final JsonElement namespace = obj.get("namespace");
            final JsonElement podName = obj.get("podName");
            if (namespace == null || podName == null) {
                return null;
            }
            return new KuberMeta(namespace.getAsString(), podName.getAsString());
        }
    }
}
