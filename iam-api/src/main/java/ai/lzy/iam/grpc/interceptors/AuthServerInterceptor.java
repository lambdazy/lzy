package ai.lzy.iam.grpc.interceptors;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.utils.TokenParser;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.OttCredentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthUnauthenticatedException;
import com.google.common.collect.ImmutableSet;
import io.grpc.*;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;

import static ai.lzy.util.grpc.GrpcHeaders.AUTHORIZATION;

public class AuthServerInterceptor implements ServerInterceptor {
    public static final Logger LOG = LogManager.getLogger(AuthServerInterceptor.class);

    private final Function<AuthException, StatusException> exceptionMapper;
    private final Set<MethodDescriptor<?, ?>> unauthenticatedMethods;
    private final AuthenticateService authenticateService;

    public AuthServerInterceptor(AuthenticateService authenticateService) {
        this(AuthServerInterceptor::defaultExceptionMapper, authenticateService);
    }

    public AuthServerInterceptor(Function<AuthException, StatusException> exceptionMapper,
                                 AuthenticateService authenticateService)
    {
        this(exceptionMapper, ImmutableSet.of(), authenticateService);
    }

    AuthServerInterceptor(Function<AuthException, StatusException> exceptionMapper,
                          Set<MethodDescriptor<?, ?>> unauthenticatedMethods,
                          AuthenticateService authenticateService)
    {
        this.exceptionMapper = exceptionMapper;
        this.unauthenticatedMethods = unauthenticatedMethods;
        this.authenticateService = authenticateService;
    }

    private static StatusException defaultExceptionMapper(AuthException e) {
        return e.status().withDescription(e.getMessage()).asException(new Metadata());
    }

    public AuthServerInterceptor withUnauthenticated(MethodDescriptor<?, ?>... unauthenticatedMethods) {
        return this.withUnauthenticated(Arrays.asList(unauthenticatedMethods));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public AuthServerInterceptor withUnauthenticated(Iterable<MethodDescriptor<?, ?>> unauthenticatedMethods) {
        ImmutableSet unauthenticatedMethodSet;
        if (this.unauthenticatedMethods.isEmpty()) {
            unauthenticatedMethodSet = ImmutableSet.copyOf(unauthenticatedMethods);
        } else {
            unauthenticatedMethodSet = ImmutableSet.builder()
                    .addAll(this.unauthenticatedMethods)
                    .addAll(unauthenticatedMethods).build();
        }

        return new AuthServerInterceptor(exceptionMapper, unauthenticatedMethodSet, authenticateService);
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(ServerCall<T, R> call, Metadata headers,
                                                       ServerCallHandler<T, R> next)
    {
        try {
            if (unauthenticatedMethods.contains(call.getMethodDescriptor())) {
                return next.startCall(call, headers);
            } else {
                AuthenticationContext authContext = authenticate(headers);
                if (authContext == null) {
                    throw new IllegalArgumentException("Authorization header is missing");
                } else {
                    Context context = Context.current().withValue(AuthenticationContext.KEY, authContext);
                    var serverCall = new GrpcServerCall<>(call, authContext.getSubject().str());
                    return Contexts.interceptCall(context, serverCall, headers, next);
                }
            }
        } catch (IllegalArgumentException iaException) {
            return this.closeCall(call, exceptionMapper.apply(new AuthUnauthenticatedException(iaException, "")));
        } catch (AuthException authException) {
            LOG.error("Auth error, status: {}, internal: {}",
                authException.status(), authException.getInternalDetails());
            return this.closeCall(call, exceptionMapper.apply(authException));
        }
    }

    @Nullable
    protected AuthenticationContext authenticate(Metadata headers) {
        String authorizationHeader = headers.get(AUTHORIZATION);
        if (authorizationHeader == null) {
            return null;
        } else {
            TokenParser.Token token = TokenParser.parse(authorizationHeader);
            Credentials credentials = switch (token.kind()) {
                case JWT -> new JwtCredentials(token.token());
                case OTT -> new OttCredentials(token.token());
            };
            Subject subject = authenticateService.authenticate(credentials);
            return new AuthenticationContext(token, credentials, subject);
        }
    }

    private <T, R> ServerCall.Listener<T> closeCall(ServerCall<T, R> call, StatusException statusException) {
        call.close(statusException.getStatus(), statusException.getTrailers());
        return new ServerCall.Listener<>() {
        };
    }

    private static class GrpcServerCall<M, R> extends ForwardingServerCall.SimpleForwardingServerCall<M, R> {
        private GrpcServerCall(ServerCall<M, R> serverCall, String subject) {
            super(serverCall);
            ThreadContext.put("subj", subject);
        }

        @Override
        public void close(Status status, Metadata trailers) {
            super.close(status, trailers);
            ThreadContext.remove("subj");
        }
    }
}
