package ai.lzy.iam.grpc.interceptors;

import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.util.auth.exceptions.AuthException;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class AccessServerInterceptor implements ServerInterceptor {
    private static final Logger LOG = LogManager.getLogger(AccessServerInterceptor.class);

    private final AccessClient accessServiceClient;

    private record AuthConf(
        AuthResource resource,
        AuthPermission permission
    ) {}

    private final AtomicReference<AuthConf> authConf = new AtomicReference<>(null);

    public AccessServerInterceptor(AccessClient accessServiceClient) {
        this.accessServiceClient = accessServiceClient;
    }

    public AccessServerInterceptor(AccessClient accessClient, AuthResource resource, AuthPermission permission) {
        this.accessServiceClient = accessClient;
        authConf.set(new AuthConf(resource, permission));
    }

    public void configure(AuthResource authResource, AuthPermission authPermission) {
        LOG.debug("Configure check access to {}/{}", authResource, authPermission);
        authConf.set(new AuthConf(authResource, authPermission));
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next)
    {
        if (!AuthenticationContext.isAuthenticated()) {
            LOG.warn("Missing authentication context!");

            call.close(Status.UNAUTHENTICATED, new Metadata());
            return new ServerCall.Listener<>() {};
        }

        var authConf = this.authConf.get();
        if (authConf == null) {
            LOG.warn("Auth resource is not configures yet");

            call.close(Status.UNAUTHENTICATED, new Metadata());
            return new ServerCall.Listener<>() {};
        }

        var auth = Objects.requireNonNull(AuthenticationContext.current());
        var subject = auth.getSubject();
        var credentials = auth.getCredentials();

        LOG.debug("Check access for {} to {}", subject, authConf);

        boolean ok;
        try {
            ok = accessServiceClient.withToken(() -> credentials)
                .hasResourcePermission(subject, authConf.resource(), authConf.permission());
        } catch (AuthException e) {
            return permissionDenied(call, subject);
        }

        if (ok) {
            return next.startCall(call, headers);
        }

        return permissionDenied(call, subject);
    }
    private <ReqT, RespT> ServerCall.Listener<ReqT> permissionDenied(ServerCall<ReqT, RespT> call, Subject subject) {
        LOG.warn("Subject '{}' is not authorized to {}", subject, authConf.get());

        call.close(Status.PERMISSION_DENIED, new Metadata());
        return new ServerCall.Listener<>() {};
    }
}
