package ru.yandex.cloud.ml.platform.lzy.model;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Context;
import io.grpc.Metadata;

public class GrpcConstant {
    public static final Metadata.Key<String> SESSION_ID_METADATA_KEY =
        Metadata.Key.of("kharon_session_id", ASCII_STRING_MARSHALLER);
    public static final Context.Key<String> SESSION_ID_CTX_KEY = Context.key("kharon_session_id");
}
