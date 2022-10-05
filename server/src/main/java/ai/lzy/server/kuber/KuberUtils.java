package ai.lzy.server.kuber;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class KuberUtils {
    private KuberUtils() {
    }

    public static Optional<V1Pod> findPodByName(V1PodList podList, String name) {
        return podList
            .getItems()
            .stream()
            .filter(v1pod -> name.equals(Objects.requireNonNull(v1pod.getMetadata()).getName()))
            .collect(Collectors.toList())
            .stream()
            .findFirst();
    }

    public static Optional<V1Container> findContainerByName(V1Pod pod, String name) {
        return Objects.requireNonNull(pod.getSpec())
            .getContainers()
            .stream()
            .filter(c -> name.equals(c.getName()))
            .findFirst();
    }

    public static String kuberValidName(String name) {
        // k8s pod name can only contain symbols [-a-z0-9]
        return name.toLowerCase(Locale.ROOT).replaceAll("[^-a-z0-9]", "-");
    }

    public static V1PodList listPods(CoreV1Api api, String namespace, String labelSelector) {
        final V1PodList podList;
        try {
            podList = api.listNamespacedPod(
                    namespace,
                    null,
                    null,
                    null,
                    null,
                    labelSelector,
                    Integer.MAX_VALUE,
                    null,
                    null,
                    null,
                    Boolean.FALSE
            );
        } catch (ApiException e) {
            throw new RuntimeException(String.format(
                    "Exception while listing pod in namespace %s with label selector %s in kuber "
                            + "exception=%s, message=%s, errorCode=%d, responseBody=%s, stackTrace=%s",
                    namespace,
                    labelSelector,
                    e,
                    e.getMessage(),
                    e.getCode(),
                    e.getResponseBody(),
                    Arrays.stream(e.getStackTrace())
                            .map(StackTraceElement::toString)
                            .collect(Collectors.joining(","))
            ));
        }
        return podList;
    }
}
