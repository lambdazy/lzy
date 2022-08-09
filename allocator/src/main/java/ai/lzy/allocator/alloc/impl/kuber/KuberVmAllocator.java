package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.model.Vm;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
@Requires(property = "allocator.kuber-allocator.enabled", value = "true")
public class KuberVmAllocator implements VmAllocator {
    private static final Logger LOG = LogManager.getLogger(KuberVmAllocator.class);
    private static final String NAMESPACE = "default";

    private final VmPodProvider provider;
    private final CoreV1Api api;

    @Inject
    public KuberVmAllocator(VmPodProvider provider) {
        this.provider = provider;
        try {
            Configuration.setDefaultApiClient(Config.defaultClient());
            api = new CoreV1Api();
        } catch (IOException e) {
            throw new RuntimeException("Cannot init KuberVmAllocator: ", e);
        }
    }

    protected KuberMeta requestAllocation(Vm vm) {
        final V1Pod servantPodSpec;
        servantPodSpec = provider.createVmPod(vm);
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
            VmPodProvider.LZY_POD_NAME_LABEL + "=" + name,
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
    public Map<String, String> allocate(Vm vm) {
        final KuberMeta meta = requestAllocation(vm);
        return meta.toMap();
    }

    @Override
    public void deallocate(Vm vm) {
        if (vm.allocatorMeta() == null) {
            throw new RuntimeException("Cannot get allocatorMeta");
        }
        final KuberMeta meta = KuberMeta.fromMap(vm.allocatorMeta());
        if (meta == null) {
            throw new RuntimeException("Cannot parse servant metadata");
        }
        terminate(meta.namespace(), meta.podName());
    }

    private record KuberMeta(String namespace, String podName) {
        Map<String, String> toMap() {
            return Map.of(
                "namespace", namespace,
                "podName", podName
            );
        }

        @Nullable
        static KuberMeta fromMap(Map<String, String> map) {
            if (!map.containsKey("namespace") || !map.containsKey("podName")) {
                return null;
            }
            final String namespace = map.get("namespace");
            final String podName = map.get("podName");
            return new KuberMeta(namespace, podName);
        }
    }
}
