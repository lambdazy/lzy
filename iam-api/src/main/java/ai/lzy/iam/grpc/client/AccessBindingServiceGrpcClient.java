package ai.lzy.iam.grpc.client;

import ai.lzy.iam.clients.AccessBindingClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LABS;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AccessBindingServiceGrpcClient implements AccessBindingClient {
    private final String clientName;
    private final Channel channel;
    private final LzyAccessBindingServiceGrpc.LzyAccessBindingServiceBlockingStub accessBindingService;
    private final Supplier<Credentials> token;

    public AccessBindingServiceGrpcClient(String clientName, Channel channel, Supplier<Credentials> token) {
        this.clientName = clientName;
        this.channel = channel;
        this.token = token;
        this.accessBindingService = GrpcUtils.newBlockingClient(
            LzyAccessBindingServiceGrpc.newBlockingStub(this.channel), clientName, () -> this.token.get().token());
    }

    @Override
    public AccessBindingClient withToken(Supplier<Credentials> tokenSupplier) {
        return new AccessBindingServiceGrpcClient(clientName, channel, tokenSupplier);
    }

    @Override
    public Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException {
        try {
            var bindings = accessBindingService.listAccessBindings(
                LABS.ListAccessBindingsRequest.newBuilder()
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
        throws AuthException
    {
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
