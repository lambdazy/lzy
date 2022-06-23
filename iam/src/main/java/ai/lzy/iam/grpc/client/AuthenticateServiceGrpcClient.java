package ai.lzy.iam.grpc.client;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.utils.GrpcConfig;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.utils.GrpcConverter;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LAS;
import ai.lzy.v1.LzyAuthenticateServiceGrpc;

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
