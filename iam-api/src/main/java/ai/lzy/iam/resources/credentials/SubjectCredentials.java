package ai.lzy.iam.resources.credentials;

import ai.lzy.iam.resources.subjects.CredentialsType;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nullable;

public record SubjectCredentials(
    String name,
    String value,
    CredentialsType type,
    @Nullable Instant expiredAt
) {
    public SubjectCredentials(String name, String value, CredentialsType type) {
        this(name, value, type, null);
    }

    public SubjectCredentials(String name, String value, CredentialsType type, @Nullable Instant expiredAt) {
        this.name = name;
        this.value = value;
        this.type = type;
        this.expiredAt = expiredAt != null ? expiredAt.truncatedTo(ChronoUnit.SECONDS) : null;
    }

    public static SubjectCredentials publicKey(String name, String value) {
        return new SubjectCredentials(name, value, CredentialsType.PUBLIC_KEY, null);
    }

    public static SubjectCredentials publicKey(String name, String value, Duration ttl) {
        return new SubjectCredentials(name, value, CredentialsType.PUBLIC_KEY, Instant.now().plus(ttl));
    }
}
