package ai.lzy.util.grpc;

import ai.lzy.v1.headers.LH;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.protobuf.ProtoUtils;
import lombok.Lombok;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public class GrpcHeaders {
    public static final Context.Key<Metadata> HEADERS = Context.key("metadata");

    public static final Metadata.Key<String> AUTHORIZATION = createMetadataKey("Authorization");
    public static final Metadata.Key<String> X_REQUEST_ID = createMetadataKey("X-Request-ID");
    public static final Metadata.Key<String> X_SUBJECT_ID = createMetadataKey("X-Subject-ID");
    public static final Metadata.Key<String> IDEMPOTENCY_KEY = createMetadataKey("Idempotency-Key");

    public static final Metadata.Key<LH.UserLogsHeader> USER_LOGS_HEADER_KEY = Metadata.Key.of(
        "User-Logs-bin",
        ProtoUtils.metadataMarshaller(LH.UserLogsHeader.getDefaultInstance())
    );

    public static Metadata getHeaders() {
        return HEADERS.get();
    }

    public static String getHeader(String headerName) {
        return getHeader(createMetadataKey(headerName));
    }

    @Nullable
    public static <T> T getHeader(Metadata.Key<T> key) {
        return getHeader(getHeaders(), key);
    }

    @Nullable
    public static <T> T getHeader(Metadata headers, Metadata.Key<T> key) {
        return headers == null ? null : headers.get(key);
    }

    public static <T> T getHeaderOrDefault(Metadata.Key<T> key, T defaultValue) {
        Objects.requireNonNull(defaultValue);
        return getHeaderOrDefault(getHeaders(), key, defaultValue);
    }

    public static <T> T getHeaderOrDefault(Metadata headers, Metadata.Key<T> key, T defaultValue) {
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

    public static ContextBuilder withContext() {
        return new ContextBuilder(true);
    }

    public static ContextBuilder withNewContext() {
        return new ContextBuilder(false);
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

    public static class ContextBuilder {
        private final Metadata metadata;

        public ContextBuilder(boolean useExisting) {
            if (useExisting) {
                metadata = Objects.requireNonNullElseGet(getHeaders(), Metadata::new);
            } else {
                metadata = new Metadata();
            }
        }

        public ContextBuilder withMeta(Metadata meta) {
            this.metadata.merge(meta);
            return this;
        }

        public <T> ContextBuilder withHeader(Metadata.Key<T> key, T value) {
            if (value != null) {
                metadata.put(key, value);
            }
            return this;
        }

        public Context build() {
            Objects.requireNonNull(metadata);
            return Context.current().withValue(HEADERS, metadata);
        }

        public void run(Runnable r) {
            withContext(build(), r);
        }

        public <T> T run(Supplier<T> fn) {
            return withContext(build(), fn);
        }
    }
}
