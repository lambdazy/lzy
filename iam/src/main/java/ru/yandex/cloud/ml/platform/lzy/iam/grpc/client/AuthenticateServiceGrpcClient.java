package ru.yandex.cloud.ml.platform.lzy.iam.grpc.client;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.AuthenticateService;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ClientHeaderInterceptor;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.GrpcHeaders;
import yandex.cloud.lzy.v1.IAM;
import yandex.cloud.lzy.v1.LAS;
import yandex.cloud.lzy.v1.LzyAuthenticateServiceGrpc;

@Singleton
public class AuthenticateServiceGrpcClient implements AuthenticateService {
    private static final Logger LOG = LogManager.getLogger(AuthenticateServiceGrpcClient.class);

    private final Channel channel;

    public AuthenticateServiceGrpcClient(GrpcConfig config) {
        this(
                ChannelBuilder.forAddress(config.host(), config.port())
                        .usePlaintext()
                        .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
                        .build()
        );
    }

    public AuthenticateServiceGrpcClient(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        try {
            LzyAuthenticateServiceGrpc.LzyAuthenticateServiceBlockingStub authenticateService =
                    LzyAuthenticateServiceGrpc.newBlockingStub(this.channel)
                            .withInterceptors(new ClientHeaderInterceptor<>(
                                    GrpcHeaders.AUTHORIZATION,
                                    credentials::token));
            final IAM.Subject subject = authenticateService.authenticate(
                    LAS.AuthenticateRequest.newBuilder().build());
            return GrpcConverter.to(subject);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

}
