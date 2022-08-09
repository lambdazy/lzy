package ai.lzy.iam.grpc.client;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.util.ChannelBuilder;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.utils.GrpcConverter;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.v1.iam.LACS;
import ai.lzy.v1.iam.LzyAccessServiceGrpc;

import java.util.function.Supplier;

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
        this.accessService = LzyAccessServiceGrpc.newBlockingStub(this.channel)
                .withInterceptors(ClientHeaderInterceptor.header(
                        GrpcHeaders.AUTHORIZATION,
                        () -> this.tokenSupplier.get().token()));
    }

    @Override
    public AccessClient withToken(Supplier<Credentials> tokenSupplier) {
        return new AccessServiceGrpcClient(this.channel, tokenSupplier);
    }

    @Override
    public boolean hasResourcePermission(
            Subject subject,
            AuthResource resource,
            AuthPermission permission) throws AuthException {
        try {
            var subj = accessService.authorize(LACS.AuthorizeRequest.newBuilder()
                    .setSubject(GrpcConverter.from(subject))
                    .setPermission(permission.permission())
                    .setResource(GrpcConverter.from(resource))
                    .build());
            return subj.getId().equals(subject.id());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }
}
