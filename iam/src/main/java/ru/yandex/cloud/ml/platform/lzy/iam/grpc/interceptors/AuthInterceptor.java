package ru.yandex.cloud.ml.platform.lzy.iam.grpc.interceptors;

import com.google.common.collect.ImmutableSet;
import io.grpc.*;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AuthenticateService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.JwtCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthUnauthenticatedException;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.context.AuthenticationContext;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.TokenParser;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;

import static ru.yandex.cloud.ml.platform.lzy.model.grpc.GrpcHeaders.AUTHORIZATION;


public class AuthInterceptor implements ServerInterceptor {

    private final Function<AuthException, StatusException> exceptionMapper;
    private final Set<MethodDescriptor<?, ?>> unauthenticatedMethods;
    private final AuthenticateService cloudAuthClient;

    public AuthInterceptor(AuthenticateService cloudAuthClient) {
        this(AuthInterceptor::defaultExceptionMapper, cloudAuthClient);
    }

    public AuthInterceptor(Function<AuthException, StatusException> exceptionMapper,
                           AuthenticateService cloudAuthClient) {
        this(exceptionMapper, ImmutableSet.of(), cloudAuthClient);
    }

    AuthInterceptor(Function<AuthException, StatusException> exceptionMapper,
                    Set<MethodDescriptor<?, ?>> unauthenticatedMethods,
                    AuthenticateService cloudAuthClient) {
        this.exceptionMapper = exceptionMapper;
        this.unauthenticatedMethods = unauthenticatedMethods;
        this.cloudAuthClient = cloudAuthClient;
    }

    private static StatusException defaultExceptionMapper(AuthException e) {
        return e.status().withDescription(e.getMessage()).asException(new Metadata());
    }

    public AuthInterceptor withUnauthenticated(MethodDescriptor<?, ?>... unauthenticatedMethods) {
        return this.withUnauthenticated(Arrays.asList(unauthenticatedMethods));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public AuthInterceptor withUnauthenticated(Iterable<MethodDescriptor<?, ?>> unauthenticatedMethods) {
        ImmutableSet unauthenticatedMethodSet;
        if (this.unauthenticatedMethods.isEmpty()) {
            unauthenticatedMethodSet = ImmutableSet.copyOf(unauthenticatedMethods);
        } else {
            unauthenticatedMethodSet = ImmutableSet.builder()
                    .addAll(this.unauthenticatedMethods)
                    .addAll(unauthenticatedMethods).build();
        }

        return new AuthInterceptor(exceptionMapper, unauthenticatedMethodSet, cloudAuthClient);
    }

    @Override
    public <T, R> ServerCall.Listener<T> interceptCall(
            ServerCall<T, R> call,
            Metadata headers,
            ServerCallHandler<T, R> next
    ) {
        try {
            if (unauthenticatedMethods.contains(call.getMethodDescriptor())) {
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
        } catch (IllegalArgumentException iaException) {
            return this.closeCall(call, exceptionMapper.apply(new AuthUnauthenticatedException(iaException, "")));
        } catch (AuthException authException) {
            return this.closeCall(call, exceptionMapper.apply(authException));
        }
    }

    protected AuthenticationContext authenticate(Metadata headers) {
        String authorizationHeader = headers.get(AUTHORIZATION);
        if (authorizationHeader == null) {
            return null;
        } else {
            TokenParser.Token token = TokenParser.parse(authorizationHeader);
            Credentials credentials;
            if (token.kind() == TokenParser.Token.Kind.JWT_TOKEN) {
                credentials = new JwtCredentials(token.token());
            } else {
                throw new IllegalStateException("Unknown kind of credentials");
            }
            Subject subject = cloudAuthClient.authenticate(credentials);
            return new AuthenticationContext(token, credentials, subject);
        }
    }

    private <T, R> ServerCall.Listener<T> closeCall(ServerCall<T, R> call, StatusException statusException) {
        call.close(statusException.getStatus(), statusException.getTrailers());
        return new ServerCall.Listener<>() {
        };
    }

}
