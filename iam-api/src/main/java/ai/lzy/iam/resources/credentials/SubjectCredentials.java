package ai.lzy.iam.resources.credentials;

import ai.lzy.iam.resources.subjects.CredentialsType;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

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
        if (type == CredentialsType.OTT && expiredAt == null) {
            throw new IllegalArgumentException("OTT must be with ttl");
        }
        this.name = name;
        this.value = value;
        this.type = type;
        this.expiredAt = expiredAt != null ? expiredAt.truncatedTo(ChronoUnit.SECONDS) : null;
    }
}
