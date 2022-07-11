package ai.lzy.server;

import ai.lzy.server.task.Task;
import java.util.UUID;
import ai.lzy.model.utils.Permissions;
import ai.lzy.priv.v2.Lzy;

public interface Authenticator {
    boolean checkUser(String userId, String token);

    boolean checkTask(String tid, String servantId, String servantToken);

    boolean canPublish(String userId);

    boolean canAccess(String zygoteName, String user);

    boolean canAccess(Task task, String user);

    String userForTask(Task task);

    void registerOperation(String zygoteName, String userId, Lzy.PublishRequest.VisibilityScope scope);

    void registerTask(String uid, Task task, String servantId);

    String registerServant(String servantId);

    boolean hasPermission(String uid, Permissions permission);

    boolean hasPermission(String uid, String permission);

    boolean checkBackOfficeSession(String sessionId, String userId);

    boolean canAccessBucket(String uid, String bucket);

    String bucketForUser(String uid);
}
