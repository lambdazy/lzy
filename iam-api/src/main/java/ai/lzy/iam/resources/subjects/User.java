package ai.lzy.iam.resources.subjects;

import java.util.Map;

public final class User extends Subject {

    public User(String id, AuthProvider provider, String providerId) {
        this(id, provider, providerId, Map.of());
    }

    public User(String id, AuthProvider provider, String providerId, Map<String, String> meta) {
        super(id, SubjectType.USER, provider, providerId, meta);
    }
}
