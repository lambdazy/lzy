package ai.lzy.iam.grpc.interceptors;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.function.Predicate;

public class AllowSubjectOnlyInterceptor implements ServerInterceptor {
    private static final Logger LOG = LogManager.getLogger(AllowSubjectOnlyInterceptor.class);

    public static final ServerInterceptor ALLOW_USER_ONLY = new AllowSubjectOnlyInterceptor(new AllowUserOnly());
    public static final ServerInterceptor ALLOW_WORKER_ONLY = new AllowSubjectOnlyInterceptor(new AllowWorkerOnly());

    private final Predicate<Subject> subjectFilter;

    public AllowSubjectOnlyInterceptor(Predicate<Subject> subjectFilter) {
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

        var allow = subjectFilter.test(subject);

        if (allow) {
            return next.startCall(call, headers);
        } else {
            LOG.warn("Subject '{}' is not allowed to call {} by the policy {}.",
                subject, call.getMethodDescriptor().getFullMethodName(), subjectFilter);

            call.close(Status.PERMISSION_DENIED, new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    public static final class AllowUserOnly implements Predicate<Subject> {
        @Override
        public boolean test(Subject subject) {
            return subject.type() == SubjectType.USER;
        }
        @Override
        public String toString() {
            return "AllowUserOnly";
        }
    }

    public static final class AllowWorkerOnly implements Predicate<Subject> {
        @Override
        public boolean test(Subject subject) {
            return subject.type() == SubjectType.WORKER;
        }
        @Override
        public String toString() {
            return "AllowWorkerOnly";
        }
    }
}
