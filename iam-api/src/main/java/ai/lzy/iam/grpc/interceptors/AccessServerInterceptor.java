package ai.lzy.iam.grpc.interceptors;

import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.exceptions.AuthException;
import io.grpc.*;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class AccessServerInterceptor implements ServerInterceptor {
    private static final Logger LOG = LogManager.getLogger(AccessServerInterceptor.class);

    private AccessClient accessServiceClient;
    private final Set<MethodDescriptor<?, ?>> exceptMethods;
    private Supplier<Credentials> tokenSupplier;

    private record AuthConf(
        AuthResource resource,
        AuthPermission permission
    ) {}

    private final AtomicReference<AuthConf> authConf;

    public AccessServerInterceptor(AccessClient accessServiceClient) {
        this(accessServiceClient, () -> new JwtCredentials("stub"), Set.of(), null);
    }

    public AccessServerInterceptor(AccessClient accessServiceClient, Set<MethodDescriptor<?, ?>> exceptMethods) {
        this(accessServiceClient, () -> new JwtCredentials("stub"), exceptMethods, null);
    }

    public AccessServerInterceptor(AccessClient accessClient, Supplier<Credentials> tokenSupplier,
                                   AuthResource resource, AuthPermission permission)
    {
        this(accessClient, tokenSupplier, Set.of(), new AuthConf(resource, permission));
    }

    public AccessServerInterceptor(AccessClient accessClient, Supplier<Credentials> tokenSupplier,
                                   Set<MethodDescriptor<?, ?>> exceptMethods,
                                   AuthResource resource, AuthPermission permission)
    {
        this(accessClient, tokenSupplier, exceptMethods, new AuthConf(resource, permission));
    }

    private AccessServerInterceptor(AccessClient accessServiceClient, Supplier<Credentials> tokenSupplier,
                                    Set<MethodDescriptor<?, ?>> exceptMethods,
                                    @Nullable AuthConf authConf)
    {
        this.tokenSupplier = tokenSupplier;
        this.accessServiceClient = accessServiceClient.withToken(tokenSupplier);
        this.exceptMethods = exceptMethods;
        this.authConf = new AtomicReference<>(authConf);
    }

    public void configure(AuthResource authResource, AuthPermission authPermission) {
        LOG.debug("Configure check access to {}/{}", authResource, authPermission);
        authConf.set(new AuthConf(authResource, authPermission));
    }

    public void configureToken(Supplier<Credentials> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
        this.accessServiceClient = accessServiceClient.withToken(tokenSupplier);
    }

    public AccessServerInterceptor ignoreMethods(MethodDescriptor<?, ?>... methods) {
        return new AccessServerInterceptor(accessServiceClient, tokenSupplier, Set.of(methods), authConf.get());
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

        if (exceptMethods.contains(call.getMethodDescriptor())) {
            return next.startCall(call, headers);
        }

        var authConf = this.authConf.get();
        if (authConf == null) {
            LOG.warn("Auth resource is not configures yet");

            call.close(Status.UNAUTHENTICATED, new Metadata());
            return new ServerCall.Listener<>() {};
        }

        var auth = Objects.requireNonNull(AuthenticationContext.current());
        var subject = auth.getSubject();

        LOG.debug("Check access for {} to {}", subject, authConf);

        boolean ok;
        try {
            ok = accessServiceClient
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
