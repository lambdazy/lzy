package ai.lzy.iam.grpc.client;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.iam.IAM;
import ai.lzy.v1.iam.LAS;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AuthenticateServiceGrpcClient implements AuthenticateService {
    private static final Logger LOG = LogManager.getLogger(AuthenticateServiceGrpcClient.class);

    private final Channel channel;

    public AuthenticateServiceGrpcClient(GrpcConfig config) {
        this(
            ChannelBuilder
                .forAddress(config.host(), config.port())
                .usePlaintext()
                .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
                .build());
    }

    public AuthenticateServiceGrpcClient(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        try {
            LzyAuthenticateServiceGrpc.LzyAuthenticateServiceBlockingStub authenticateService =
                    LzyAuthenticateServiceGrpc.newBlockingStub(this.channel)
                            .withInterceptors(ClientHeaderInterceptor.header(
                                    GrpcHeaders.AUTHORIZATION,
                                    credentials::token));
            final IAM.Subject subject = authenticateService.authenticate(
                    LAS.AuthenticateRequest.newBuilder().build());
            return ProtoConverter.to(subject);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

}
