package ai.lzy.allocator.util;

import io.fabric8.kubernetes.client.KubernetesClientException;

import java.net.HttpURLConnection;

public final class KuberUtils {
    private KuberUtils() {
    }

    public static boolean isResourceAlreadyExist(Exception e) {
        return e instanceof KubernetesClientException ex && ex.getCode() == HttpURLConnection.HTTP_CONFLICT;
    }

    public static boolean isNotRetryable(KubernetesClientException e) {
        var code = e.getCode();
        return code == HttpURLConnection.HTTP_BAD_REQUEST ||
            code == HttpURLConnection.HTTP_UNAUTHORIZED ||
            code == HttpURLConnection.HTTP_FORBIDDEN ||
            code == HttpURLConnection.HTTP_NOT_FOUND ||
            code == HttpURLConnection.HTTP_BAD_METHOD;
    }
}
