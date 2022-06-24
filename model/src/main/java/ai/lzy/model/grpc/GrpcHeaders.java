package ai.lzy.model.grpc;

import io.grpc.Metadata;

public class GrpcHeaders {
    public static final Metadata.Key<String> AUTHORIZATION = createMetadataKey("Authorization");
    public static final Metadata.Key<String> X_REQUEST_ID = createMetadataKey("X-Request-ID");
    public static final Metadata.Key<String> X_SUBJECT_ID = createMetadataKey("X-Subject-ID");

    private static io.grpc.Metadata.Key<String> createMetadataKey(String headerName) {
        return io.grpc.Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
    }
}
