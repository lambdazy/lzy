package ai.lzy.iam.grpc.interceptors;

import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.JwtCredentials;
import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class AllowSubjectOnlyInterceptor implements ServerInterceptor {
    private static final Logger LOG = LogManager.getLogger(AllowSubjectOnlyInterceptor.class);

    public static final ServerInterceptor ALLOW_USER_ONLY = new AllowSubjectOnlyInterceptor(new AllowUserOnly());
    public static final ServerInterceptor ALLOW_WORKER_ONLY = new AllowSubjectOnlyInterceptor(new AllowWorkerOnly());

    private final Predicate<AuthenticationContext> subjectFilter;

    public AllowSubjectOnlyInterceptor(Predicate<AuthenticationContext> subjectFilter) {
        this.subjectFilter = subjectFilter;
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

        var auth = Objects.requireNonNull(AuthenticationContext.current());
        var subject = auth.getSubject();

        var allow = subjectFilter.test(auth);

        if (allow) {
            return next.startCall(call, headers);
        } else {
            LOG.warn("Subject '{}' is not allowed to call {} by the policy {}.",
                subject, call.getMethodDescriptor().getFullMethodName(), subjectFilter);

            call.close(Status.PERMISSION_DENIED, new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    public static final class AllowUserOnly implements Predicate<AuthenticationContext> {
        @Override
        public boolean test(AuthenticationContext ctx) {
            return ctx.getSubject().type() == SubjectType.USER;
        }
        @Override
        public String toString() {
            return "AllowUserOnly";
        }
    }

    public static final class AllowWorkerOnly implements Predicate<AuthenticationContext> {
        @Override
        public boolean test(AuthenticationContext ctx) {
            return ctx.getSubject().type() == SubjectType.WORKER;
        }
        @Override
        public String toString() {
            return "AllowWorkerOnly";
        }
    }

    public static AllowSubjectOnlyInterceptor withPermissions(Map<AuthResource, AuthPermission> permissions,
                                                              String clientName, Channel iamChannel)
    {
        var client = new AccessServiceGrpcClient(clientName, iamChannel, () -> new JwtCredentials("stub"));

        return new AllowSubjectOnlyInterceptor(auth -> permissions.entrySet().stream().allMatch(entry -> {
            var resource = entry.getKey();
            var permission = entry.getValue();

            try {
                return client
                    .withToken(auth::getCredentials)
                    .hasResourcePermission(auth.getSubject(), resource, permission);
            } catch (Exception e) {
                LOG.error("Failed to check permission for subject '{}' on resource '{}' with permission '{}'.",
                    auth.getSubject(), resource, permission, e);
                return false;
            }
        }));
    }
}
