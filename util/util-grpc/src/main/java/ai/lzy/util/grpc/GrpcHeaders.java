package ai.lzy.util.grpc;

import io.grpc.Context;
import io.grpc.Metadata;
import lombok.Lombok;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public class GrpcHeaders {
    public static final Context.Key<Metadata> HEADERS = Context.key("metadata");

    public static final Metadata.Key<String> AUTHORIZATION = createMetadataKey("Authorization");
    public static final Metadata.Key<String> X_REQUEST_ID = createMetadataKey("X-Request-ID");
    public static final Metadata.Key<String> X_SUBJECT_ID = createMetadataKey("X-Subject-ID");
    public static final Metadata.Key<String> IDEMPOTENCY_KEY = createMetadataKey("Idempotency-Key");

    public static Metadata getHeaders() {
        return HEADERS.get();
    }

    public static String getHeader(String headerName) {
        return getHeader(createMetadataKey(headerName));
    }

    public static String getHeader(Metadata.Key<String> key) {
        return getHeader(getHeaders(), key);
    }

    public static String getHeader(Metadata headers, Metadata.Key<String> key) {
        return headers == null ? null : headers.get(key);
    }

    public static String getHeaderOrDefault(Metadata.Key<String> key, String defaultValue) {
        Objects.requireNonNull(defaultValue);
        return getHeaderOrDefault(getHeaders(), key, defaultValue);
    }

    public static String getHeaderOrDefault(Metadata headers, Metadata.Key<String> key, String defaultValue) {
        Objects.requireNonNull(defaultValue);
        return Optional.ofNullable(headers).map(m -> m.get(key)).orElse(defaultValue);
    }

    public static String getIdempotencyKey() {
        return getHeader(IDEMPOTENCY_KEY);
    }

    public static String getRequestId() {
        return getHeader(X_REQUEST_ID);
    }

    public static Context createContext(Map<Metadata.Key<String>, String> overrideHeaders) {
        Metadata newHeaders = new Metadata();
        Metadata existingHeaders = getHeaders();
        if (existingHeaders != null) {
            newHeaders.merge(existingHeaders);
        }

        Objects.requireNonNull(newHeaders);
        overrideHeaders.forEach(newHeaders::put);
        return Context.current().withValue(HEADERS, newHeaders);
    }

    public static <T> T withContext(Context ctx, Supplier<T> fn) {
        try {
            return ctx.wrap(fn::get).call();
        } catch (Exception e) {
            throw Lombok.sneakyThrow(e);
        }
    }

    public static void withContext(Context ctx, Runnable fn) {
        ctx.wrap(fn).run();
    }

    private static Metadata.Key<String> createMetadataKey(String headerName) {
        return Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
    }

    public static void removeAll(Metadata.Key<String> key) {
        Metadata headers = getHeaders();
        if (headers != null) {
            headers.removeAll(key);
        }
    }
}
