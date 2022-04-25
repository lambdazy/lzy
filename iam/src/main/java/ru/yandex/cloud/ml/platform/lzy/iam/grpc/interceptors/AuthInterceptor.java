package ru.yandex.cloud.ml.platform.lzy.iam.grpc.interceptors;

import com.google.common.collect.ImmutableSet;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.StatusException;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.context.AuthenticationContext;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.TokenParser;
import yandex.cloud.lzy.v1.IAM;


public class AuthInterceptor implements ServerInterceptor {
    static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of(
            "Authorization", Metadata.ASCII_STRING_MARSHALLER
    );
    private final Function<AuthException, StatusException> exceptionMapper;
    private final Set<MethodDescriptor<?, ?>> unauthenticatedMethods;

    public AuthInterceptor() {
        this(AuthInterceptor::defaultExceptionMapper);
    }

    public AuthInterceptor(Function<AuthException, StatusException> exceptionMapper) {
        this(exceptionMapper, ImmutableSet.of());
    }

    AuthInterceptor(Function<AuthException, StatusException> exceptionMapper, Set<MethodDescriptor<?, ?>> unauthenticatedMethods) {
        this.exceptionMapper = exceptionMapper;
        this.unauthenticatedMethods = unauthenticatedMethods;
    }

    private static StatusException defaultExceptionMapper(AuthException e) {
        return e.status().withDescription(e.getMessage()).asException(new Metadata());
    }

    public AuthInterceptor withUnauthenticated(MethodDescriptor<?, ?>... unauthenticatedMethods) {
        return this.withUnauthenticated((Iterable) Arrays.asList(unauthenticatedMethods));
    }

    @SuppressWarnings("rawtypes")
    public AuthInterceptor withUnauthenticated(Iterable<MethodDescriptor<?, ?>> unauthenticatedMethods) {
        ImmutableSet unauthenticatedMethodSet;
        if (this.unauthenticatedMethods.isEmpty()) {
            unauthenticatedMethodSet = ImmutableSet.copyOf(unauthenticatedMethods);
        } else {
            unauthenticatedMethodSet = ImmutableSet.builder().addAll(this.unauthenticatedMethods).addAll(unauthenticatedMethods).build();
        }

        return new AuthInterceptor(this.exceptionMapper, unauthenticatedMethodSet);
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(
            ServerCall<T, R> call,
            Metadata headers,
            ServerCallHandler<T, R> next
    ) {
        try {
            if (this.unauthenticatedMethods.contains(call.getMethodDescriptor())) {
                return next.startCall(call, headers);
            } else {
                AuthenticationContext authContext = authenticate(headers);
                if (authContext == null) {
                    throw new IllegalArgumentException("Authorization header is missing");
                } else {
                    Context context = Context.current().withValue(AuthenticationContext.KEY, authContext);
                    return Contexts.interceptCall(context, call, headers, next);
                }
            }
        } catch (IllegalArgumentException var6) {
            return this.closeCall(call, (StatusException) this.exceptionMapper.apply(new CloudAuthUnauthenticatedException(var6, "")));
        } catch (CloudAuthException var7) {
            return this.closeCall(call, (StatusException) this.exceptionMapper.apply(var7));
        }
    }

    protected AuthenticationContext authenticate(Metadata headers) {
        String authorizationHeader = (String) headers.get(AUTHORIZATION);
        if (authorizationHeader == null) {
            return null;
        } else {
            TokenParser.Token token = TokenParser.parse(authorizationHeader);
            AbstractCredentials credentials;
            if (token.kind() == TokenParser.Token.Kind.JWT_TOKEN) {
                credentials = new IamToken(token.token());
            } else {
                throw new IllegalStateException("Unknown kind of credentials");
            }
            IAM.Subject subject = this.cloudAuthClient.authenticate(credentials);
            return new AuthenticationContext(token, credentials, subject);
        }
    }

    private <T, R> ServerCall.Listener<T> closeCall(ServerCall<T, R> call, StatusException statusException) {
        call.close(statusException.getStatus(), statusException.getTrailers());
        return new ServerCall.Listener<T>() {
        };
    }

}
