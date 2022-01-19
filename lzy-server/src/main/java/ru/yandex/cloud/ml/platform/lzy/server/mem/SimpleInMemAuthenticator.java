package ru.yandex.cloud.ml.platform.lzy.server.mem;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Singleton
@Requires(property = "authenticator", value = "SimpleInMemAuthenticator", defaultValue = "SimpleInMemAuthenticator")
public class SimpleInMemAuthenticator implements Authenticator {
    private final Map<String, String> taskTokens = new HashMap<>();
    private final Map<String, String> owners = new HashMap<>();

    @Inject
    private StorageConfigs storageConfigs;

    @Override
    public boolean checkUser(String userId, String token) {
        return true;
    }

    @Override
    public boolean checkTask(String tid, String token) {
        return token.equals(taskTokens.get(tid));
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
        return owners.get(task.tid().toString());
    }

    @Override
    public void registerOperation(String zygoteName, String userId, Lzy.PublishRequest.VisibilityScope scope) {
    }

    @Override
    public String registerTask(String uid, Task task) {
        owners.put(task.tid().toString(), uid);
        final String token = UUID.randomUUID().toString();
        taskTokens.put(task.tid().toString(), token);
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
    public boolean checkBackOfficeSession(UUID sessionId, String userId) {
        return true;
    }

    @Override
    public String bucketForUser(String uid) {
        return storageConfigs.getBucket();
    }
}
