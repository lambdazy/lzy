package ai.lzy.iam.resources.subjects;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

public abstract sealed class Subject
    permits User, Worker
{
    private final String id;
    private final SubjectType type;
    private final AuthProvider provider;
    private final String providerId;
    private final Map<String, String> meta;

    public static Subject of(String id, SubjectType type, AuthProvider provider, String providerId) {
        return of(id, type, provider, providerId, Map.of());
    }

    public static Subject of(String id, SubjectType type, AuthProvider provider, String providerId,
                             Map<String, String> meta)
    {
        return switch (type) {
            case USER -> new User(id, provider, providerId, meta);
            case WORKER -> new Worker(id, provider, providerId, meta);
        };
    }

    protected Subject(String id, SubjectType type, AuthProvider provider, String providerId, Map<String, String> meta) {
        this.id = id;
        this.type = type;
        this.provider = provider;
        this.providerId = providerId;
        this.meta = ImmutableMap.copyOf(meta);

        if (!provider.allowMetadata() && !meta.isEmpty()) {
            throw new IllegalArgumentException(this + " has non empty metadata: " + meta);
        }
    }

    public String id() {
        return id;
    }

    public SubjectType type() {
        return type;
    }

    public AuthProvider provider() {
        return provider;
    }

    public String providerId() {
        return providerId;
    }

    public Map<String, String> meta() {
        return meta;
    }

    @Override
    public String toString() {
        return "%s(%s, %s, %s, %s)".formatted(type.name(), id, provider, providerId, meta);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subject subject = (Subject) o;
        return Objects.equals(id, subject.id) && type == subject.type &&
            provider == subject.provider && Objects.equals(providerId, subject.providerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, provider, providerId);
    }
}
