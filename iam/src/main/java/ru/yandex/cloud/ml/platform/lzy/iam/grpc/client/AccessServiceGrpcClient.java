package ru.yandex.cloud.ml.platform.lzy.iam.grpc.client;

import io.grpc.Channel;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.lzy.v1.LzyAccessServiceGrpc;

@Singleton
public class AccessServiceGrpcClient {

    public static final Logger LOG = LogManager.getLogger(AccessServiceGrpcClient.class);

    private final Channel channel;

    public AccessServiceGrpcClient(GrpcConfig config) {
        channel = ChannelBuilder.forAddress(config.host(), config.port())
                .usePlaintext()
                .enableRetry(LzyAccessServiceGrpc.SERVICE_NAME)
                .build();
    }
}
