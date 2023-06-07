package ai.lzy.iam.resources.subjects;

import java.util.Map;

public final class Worker extends Subject {

    public Worker(String id, AuthProvider provider, String providerId) {
        this(id, provider, providerId, Map.of());
    }

    public Worker(String id, AuthProvider provider, String providerId, Map<String, String> meta) {
        super(id, SubjectType.WORKER, provider, providerId, meta);
    }
}
