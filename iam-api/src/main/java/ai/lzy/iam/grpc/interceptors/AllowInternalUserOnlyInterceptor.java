package ai.lzy.iam.grpc.interceptors;

import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.exceptions.AuthException;
import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class AllowInternalUserOnlyInterceptor implements ServerInterceptor {
    private static final Logger LOG = LogManager.getLogger(AllowInternalUserOnlyInterceptor.class);

    private final AccessClient accessServiceClient;

    public AllowInternalUserOnlyInterceptor(String clientName, Channel iamChannel) {
        this(new AccessServiceGrpcClient(clientName, iamChannel, () -> new JwtCredentials("i-am-a-hacker")));
    }

    public AllowInternalUserOnlyInterceptor(AccessClient accessServiceClient) {
        this.accessServiceClient = accessServiceClient;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next)
    {
        if (!AuthenticationContext.isAuthenticated()) {
            return unauthenticated(call);
        }

        var auth = Objects.requireNonNull(AuthenticationContext.current());
        var subject = auth.getSubject();
        var credentials = auth.getCredentials();

        LOG.debug("Test whether the user '{}' is the internal user.", subject);

        boolean ok;
        try {
            ok = accessServiceClient.withToken(() -> credentials)
                .hasResourcePermission(subject, Root.INSTANCE, AuthPermission.INTERNAL_AUTHORIZE);
        } catch (AuthException e) {
            return permissionDenied(call, subject);
        }

        if (ok) {
            return next.startCall(call, headers);
        }

        return permissionDenied(call, subject);
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> permissionDenied(ServerCall<ReqT, RespT> call, Subject subject) {
        LOG.warn("Subject '{}' is not the internal user, sorryan.", subject.id());

        call.close(Status.PERMISSION_DENIED, new Metadata());
        return new ServerCall.Listener<>() {};
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> unauthenticated(ServerCall<ReqT, RespT> call) {
        LOG.warn("Missing authentication context!");

        call.close(Status.UNAUTHENTICATED, new Metadata());
        return new ServerCall.Listener<>() {};
    }
}
