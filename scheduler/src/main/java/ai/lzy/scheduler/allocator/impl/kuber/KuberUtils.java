package ai.lzy.scheduler.allocator.impl.kuber;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class KuberUtils {
    private KuberUtils() {
    }

    @NotNull
    public static Optional<V1Pod> findPodByName(V1PodList podList, String name) {
        return podList
            .getItems()
            .stream()
            .filter(v1pod -> name.equals(Objects.requireNonNull(v1pod.getMetadata()).getName()))
            .findFirst();
    }

    @NotNull
    public static Optional<V1Container> findContainerByName(V1Pod pod, String name) {
        return Objects.requireNonNull(pod.getSpec())
            .getContainers()
            .stream()
            .filter(c -> name.equals(c.getName()))
            .findFirst();
    }
}
