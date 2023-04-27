package ai.lzy.allocator.util;

import io.fabric8.kubernetes.client.KubernetesClientException;

import java.net.HttpURLConnection;

public final class KuberUtils {

    public static final int HTTP_TOO_MANY_REQUESTS = 429;

    private KuberUtils() {
    }

    public static boolean isResourceAlreadyExist(Exception e) {
        return e instanceof KubernetesClientException ex && ex.getCode() == HttpURLConnection.HTTP_CONFLICT;
    }

    public static boolean isNotRetryable(KubernetesClientException e) {
        var code = e.getCode();
        if (code == HTTP_TOO_MANY_REQUESTS) {
            return false;
        }
        return code >= 400 && code < 500;
    }
}
