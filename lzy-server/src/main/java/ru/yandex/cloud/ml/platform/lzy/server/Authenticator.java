package ru.yandex.cloud.ml.platform.lzy.server;

import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.util.UUID;

public interface Authenticator {
    boolean checkUser(String userId, String token, String tokenSign);
    boolean checkTask(String tid, String token);

    boolean canPublish(String userId);
    boolean canAccess(String zygoteName, String user);
    boolean canAccess(Task task, String user);

    String userForTask(Task task);

    void registerOperation(String zygoteName, String userId, Lzy.PublishRequest.VisibilityScope scope);

    String registerTask(String uid, Task task);

    boolean hasPermission(String uid, Permissions permission);

    boolean checkBackOfficeSession(UUID sessionId, String userId);
}
