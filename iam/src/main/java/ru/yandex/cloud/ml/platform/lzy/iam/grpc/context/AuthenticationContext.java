package ru.yandex.cloud.ml.platform.lzy.iam.grpc.context;

import io.grpc.Context;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.TokenParser;
import yandex.cloud.lzy.v1.IAM;

public class AuthenticationContext {
    public static Context.Key<AuthenticationContext> KEY = Context.key("authentication-context");

    private final TokenParser.Token token;
    private final AbstractCredentials credentials;
    private final IAM.Subject subject;

    public AuthenticationContext(
            @Nonnull TokenParser.Token token,
            @Nonnull AbstractCredentials credentials,
            @Nonnull IAM.Subject subject
    ) {
        this.token =  Objects.requireNonNull(token, "token is null");
        this.credentials = Objects.requireNonNull(credentials, "credentials is null");
        this.subject = Objects.requireNonNull(subject, "subject is null");
    }

    @Nonnull
    public TokenParser.Token getToken() {
        return this.token;
    }

    @Nonnull
    public AbstractCredentials getCredentials() {
        return this.credentials;
    }

    @Nonnull
    public Subject getSubject() {
        return this.subject;
    }

    @Nullable
    public static AuthenticationContext current() {
        return KEY.get();
    }

    public static boolean isAuthenticated() {
        return current() != null;
    }

    @Nonnull
    public static Subject currentSubject() {
        AuthenticationContext ctx = current();
        if (ctx == null) {
            throw new IllegalStateException("Must be called in AuthenticationContext context!");
        } else {
            return ctx.getSubject();
        }
    }
}
