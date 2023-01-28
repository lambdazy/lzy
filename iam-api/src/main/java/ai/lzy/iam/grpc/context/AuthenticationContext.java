package ai.lzy.iam.grpc.context;

import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.utils.TokenParser;
import ai.lzy.util.auth.credentials.Credentials;
import io.grpc.Context;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AuthenticationContext {
    public static final Context.Key<AuthenticationContext> KEY = Context.key("authentication-context");

    private final TokenParser.Token token;
    private final Credentials credentials;
    private final Subject subject;

    public AuthenticationContext(TokenParser.Token token, Credentials credentials, Subject subject) {
        this.token = Objects.requireNonNull(token, "token is null");
        this.credentials = Objects.requireNonNull(credentials, "credentials is null");
        this.subject = Objects.requireNonNull(subject, "subject is null");
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

    @Nonnull
    public TokenParser.Token getToken() {
        return this.token;
    }

    @Nonnull
    public Credentials getCredentials() {
        return this.credentials;
    }

    @Nonnull
    public Subject getSubject() {
        return this.subject;
    }
}
