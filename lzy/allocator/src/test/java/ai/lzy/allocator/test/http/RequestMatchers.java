package ai.lzy.allocator.test.http;

import okhttp3.mockwebserver.RecordedRequest;

import java.util.function.Predicate;

public final class RequestMatchers {
    private RequestMatchers() {
    }

    public static Predicate<RecordedRequest> exactPath(String path) {
        return request -> request.getPath() != null && request.getPath().equals(path);
    }

    public static Predicate<RecordedRequest> method(String method) {
        return request -> request.getMethod() != null && request.getMethod().equals(method);
    }

    public static Predicate<RecordedRequest> startsWithPath(String path) {
        return request -> request.getPath() != null && request.getPath().startsWith(path);
    }

    public static Predicate<RecordedRequest> containsPath(String path) {
        return request -> request.getPath() != null && request.getPath().contains(path);
    }

    public static Predicate<RecordedRequest> containsHeader(String header, String value) {
        return request -> request.getHeader(header) != null && request.getHeader(header).equals(value);
    }
}
