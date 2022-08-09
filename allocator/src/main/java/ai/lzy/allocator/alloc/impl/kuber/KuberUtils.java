package ai.lzy.allocator.alloc.impl.kuber;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Optional;

public class KuberUtils {
    private KuberUtils() {
    }

    public static Optional<V1Pod> findPodByName(V1PodList podList, String name) {
        return podList
            .getItems()
            .stream()
            .filter(v1pod -> name.equals(Objects.requireNonNull(v1pod.getMetadata()).getName()))
            .findFirst();
    }
}
