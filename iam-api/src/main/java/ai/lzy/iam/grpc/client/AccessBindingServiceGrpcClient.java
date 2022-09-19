package ai.lzy.iam.grpc.client;

import ai.lzy.iam.clients.AccessBindingClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.iam.LABS;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AccessBindingServiceGrpcClient implements AccessBindingClient {
    private static final Logger LOG = LogManager.getLogger(AccessServiceGrpcClient.class);

    private final Channel channel;
    private final LzyAccessBindingServiceGrpc.LzyAccessBindingServiceBlockingStub accessBindingService;
    private final Supplier<Credentials> tokenSupplier;

    public AccessBindingServiceGrpcClient(GrpcConfig config, Supplier<Credentials> tokenSupplier) {
        this(
                ChannelBuilder.forAddress(config.host(), config.port())
                        .usePlaintext()
                        .enableRetry(LzyAccessBindingServiceGrpc.SERVICE_NAME)
                        .build(),
                tokenSupplier
        );
    }

    public AccessBindingServiceGrpcClient(Channel channel, Supplier<Credentials> tokenSupplier) {
        this.channel = channel;
        this.tokenSupplier = tokenSupplier;
        this.accessBindingService = LzyAccessBindingServiceGrpc.newBlockingStub(this.channel)
                .withInterceptors(ClientHeaderInterceptor.header(
                        GrpcHeaders.AUTHORIZATION,
                        () -> this.tokenSupplier.get().token()));
    }

    @Override
    public AccessBindingClient withToken(Supplier<Credentials> tokenSupplier) {
        return new AccessBindingServiceGrpcClient(this.channel, tokenSupplier);
    }

    @Override
    public Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException {
        try {
            var bindings = accessBindingService.listAccessBindings(LABS.ListAccessBindingsRequest.newBuilder()
                    .setResource(ProtoConverter.from(resource))
                    .build());
            return bindings.getBindingsList().stream().map(ProtoConverter::to);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) throws AuthException {
        try {
            LABS.SetAccessBindingsRequest.Builder requestBuilder = LABS.SetAccessBindingsRequest.newBuilder()
                    .setResource(ProtoConverter.from(resource));
            accessBinding.forEach(b -> requestBuilder.addBindings(ProtoConverter.from(b)));
            // Empty Response, see lzy-access-binding-service.proto
            var response = accessBindingService.setAccessBindings(requestBuilder.build());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas)
            throws AuthException {
        try {
            LABS.UpdateAccessBindingsRequest.Builder requestBuilder = LABS.UpdateAccessBindingsRequest.newBuilder()
                    .setResource(ProtoConverter.from(resource));
            accessBindingDeltas.forEach(b -> requestBuilder.addDeltas(ProtoConverter.from(b)));
            var response = accessBindingService.updateAccessBindings(requestBuilder.build());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }
}
