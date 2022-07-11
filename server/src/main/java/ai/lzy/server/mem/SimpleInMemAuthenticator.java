package ai.lzy.server.mem;

import ai.lzy.server.configs.StorageConfigs;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import ai.lzy.model.utils.Permissions;
import ai.lzy.server.Authenticator;
import ai.lzy.server.task.Task;
import ai.lzy.priv.v2.Lzy;

@Singleton
@Requires(property = "database.enabled", value = "false", defaultValue = "false")
public class SimpleInMemAuthenticator implements Authenticator {
    private final Map<String, String> servantTokens = new HashMap<>();
    private final Map<String, String> task2servants = new HashMap<>();
    private final Map<String, String> owners = new HashMap<>();

    @Inject
    private StorageConfigs storageConfigs;

    @Override
    public boolean checkUser(String userId, String token) {
        return true;
    }

    @Override
    public boolean checkTask(String tid, String servantId, String servantToken) {
        return servantToken.equals(servantTokens.get(servantId))
            && (tid == null || servantId.equals(task2servants.get(tid)));
    }

    @Override
    public boolean canPublish(String userId) {
        return true;
    }

    @Override
    public boolean canAccess(String zyName, String user) {
        return true;
    }

    @Override
    public boolean canAccess(Task task, String user) {
        return true;
    }

    @Override
    public String userForTask(Task task) {
        return owners.get(task.tid());
    }

    @Override
    public void registerOperation(String zygoteName, String userId, Lzy.PublishRequest.VisibilityScope scope) {
    }

    @Override
    public void registerTask(String uid, Task task, String servantId) {
        owners.put(task.tid(), uid);
        task2servants.put(task.tid(), servantId);
    }

    @Override
    public String registerServant(String servantId) {
        final String token = "servant_token_" + UUID.randomUUID();
        servantTokens.put(servantId, token);
        return token;
    }

    @Override
    public boolean hasPermission(String uid, Permissions permission) {
        return true;
    }

    @Override
    public boolean hasPermission(String uid, String permission) {
        return true;
    }

    @Override
    public boolean checkBackOfficeSession(String sessionId, String userId) {
        return true;
    }

    @Override
    public boolean canAccessBucket(String uid, String bucket) {
        return true;
    }

    @Override
    public String bucketForUser(String uid) {
        if (!storageConfigs.isSeparated()) {
            return storageConfigs.getBucket();
        }
        return uid.toLowerCase(Locale.ROOT);
    }
}
