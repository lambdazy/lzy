package ru.yandex.cloud.ml.platform.lzy.iam.grpc.client;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ClientHeaderInterceptor;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.GrpcHeaders;
import yandex.cloud.priv.lzy.v1.LAS;
import yandex.cloud.priv.lzy.v1.LzyAccessServiceGrpc;

import java.util.function.Supplier;

@Singleton
public class AccessServiceGrpcClient implements AccessClient {
    private static final Logger LOG = LogManager.getLogger(AccessServiceGrpcClient.class);

    private final Channel channel;
    private final LzyAccessServiceGrpc.LzyAccessServiceBlockingStub accessService;
    private final Supplier<Credentials> tokenSupplier;

    public AccessServiceGrpcClient(GrpcConfig config, Supplier<Credentials> tokenSupplier) {
        this(
                ChannelBuilder.forAddress(config.host(), config.port())
                        .usePlaintext()
                        .enableRetry(LzyAccessServiceGrpc.SERVICE_NAME)
                        .build(),
                tokenSupplier
        );
    }

    public AccessServiceGrpcClient(Channel channel, Supplier<Credentials> tokenSupplier) {
        this.channel = channel;
        this.tokenSupplier = tokenSupplier;
        accessService = LzyAccessServiceGrpc.newBlockingStub(this.channel)
                .withInterceptors(new ClientHeaderInterceptor<>(
                        GrpcHeaders.AUTHORIZATION,
                        () -> this.tokenSupplier.get().token()));
    }

    @Override
    public AccessClient withToken(Supplier<Credentials> tokenSupplier) {
        return new AccessServiceGrpcClient(this.channel, tokenSupplier);
    }

    @Override
    public boolean hasResourcePermission(Subject subject, AuthResource resource, AuthPermission permission) throws AuthException {
        try {
            var subj = accessService.authorize(LAS.AuthorizeRequest.newBuilder()
                    .setPermission(permission.permission())
                    .setResource(GrpcConverter.from(resource))
                    .build());
            return subj.getId().equals(subject.id());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }
}
