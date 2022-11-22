package ai.lzy.allocator.alloc.impl.kuber;

import io.fabric8.kubernetes.client.KubernetesClientException;

import java.net.HttpURLConnection;

public final class KuberUtils {
    private KuberUtils() {
    }

    public static boolean isResourceAlreadyExist(Exception e) {
        return e instanceof KubernetesClientException ex && ex.getCode() == HttpURLConnection.HTTP_CONFLICT;
    }
}
