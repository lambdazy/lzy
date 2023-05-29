package ai.lzy.iam.grpc.client;

import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LACS;
import ai.lzy.v1.iam.LzyAccessServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import java.util.function.Supplier;

public class AccessServiceGrpcClient implements AccessClient {
    private final String clientName;
    private final Channel channel;
    private final LzyAccessServiceGrpc.LzyAccessServiceBlockingStub accessService;

    public AccessServiceGrpcClient(String clientName, Channel channel, Supplier<Credentials> tokenSupplier) {
        this.clientName = clientName;
        this.channel = channel;
        this.accessService = GrpcUtils.newBlockingClient(
            LzyAccessServiceGrpc.newBlockingStub(channel), clientName, () -> tokenSupplier.get().token());
    }

    public AccessServiceGrpcClient(String clientName, Channel channel) {
        this(clientName, channel, () -> new JwtCredentials("stub"));
    }

    @Override
    public AccessClient withToken(Supplier<Credentials> tokenSupplier) {
        return new AccessServiceGrpcClient(clientName, channel, tokenSupplier);
    }

    @Override
    public boolean hasResourcePermission(Subject subject, AuthResource resource, AuthPermission permission)
        throws AuthException
    {
        try {
            var subj = accessService.authorize(LACS.AuthorizeRequest.newBuilder()
                    .setSubject(ProtoConverter.from(subject))
                    .setPermission(permission.permission())
                    .setResource(ProtoConverter.from(resource))
                    .build());
            return subj.getId().equals(subject.id());
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }
}
