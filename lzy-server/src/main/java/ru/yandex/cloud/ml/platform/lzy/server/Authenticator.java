package ru.yandex.cloud.ml.platform.lzy.server;

import java.util.UUID;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

public interface Authenticator {
    boolean checkUser(String userId, String token);

    boolean checkTask(String tid, String token);

    boolean canPublish(String userId);

    boolean canAccess(String zygoteName, String user);

    boolean canAccess(Task task, String user);

    String userForTask(Task task);

    void registerOperation(String zygoteName, String userId, Lzy.PublishRequest.VisibilityScope scope);

    String registerTask(String uid, Task task);

    boolean hasPermission(String uid, Permissions permission);

    boolean hasPermission(String uid, String permission);

    boolean checkBackOfficeSession(UUID sessionId, String userId);

    boolean canAccessBucket(String uid, String bucket);

    String bucketForUser(String uid);
}
