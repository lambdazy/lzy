package ru.yandex.cloud.ml.platform.lzy.iam.grpc.client;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.AccessBindingClient;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBinding;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBindingDelta;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ClientHeaderInterceptor;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.GrpcHeaders;
import yandex.cloud.lzy.v1.LABS;
import yandex.cloud.lzy.v1.LzyAccessBindingServiceGrpc;

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
        accessBindingService = LzyAccessBindingServiceGrpc.newBlockingStub(this.channel)
                .withInterceptors(new ClientHeaderInterceptor<>(
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
                    .setResource(GrpcConverter.from(resource))
                    .build());
            return bindings.getBindingsList().stream().map(GrpcConverter::to);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }

    @Override
    public void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) throws AuthException {
        try {
            LABS.SetAccessBindingsRequest.Builder requestBuilder = LABS.SetAccessBindingsRequest.newBuilder()
                    .setResource(GrpcConverter.from(resource));
            accessBinding.forEach(b -> requestBuilder.addBindings(GrpcConverter.from(b)));
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
                    .setResource(GrpcConverter.from(resource));
            accessBindingDeltas.forEach(b -> requestBuilder.addDeltas(GrpcConverter.from(b)));
            var response = accessBindingService.updateAccessBindings(requestBuilder.build());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }
}
