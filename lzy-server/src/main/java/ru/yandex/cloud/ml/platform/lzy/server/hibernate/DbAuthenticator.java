package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.Permissions;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.*;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.io.StringReader;
import java.security.Security;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static ru.yandex.cloud.ml.platform.lzy.model.utils.Credentials.checkToken;


@Singleton
@Requires(property = "authenticator", value = "DbAuthenticator")
public class DbAuthenticator implements Authenticator {
    private static final Logger LOG = LogManager.getLogger(DbAuthenticator.class);

    @Inject
    private DbStorage storage;

    @Override
    public boolean checkUser(String userId, String token) {
        LOG.info("checkUser userId=" + userId);
        String[] tokenParts = token.split("\\.");
        return isUserTokenSigned(userId, tokenParts[0], tokenParts[1]);
    }

    @Override
    public boolean checkTask(String tid, String token) {
        LOG.info("checkTask tid=" + tid);
        try (Session session = storage.getSessionFactory().openSession()) {
            TaskModel taskModel = session.find(TaskModel.class, UUID.fromString(tid));
            return taskModel.getToken().equals(token);
        }
    }

    @Override
    public boolean canPublish(String userId) {
        return true;
    }

    @Override
    public boolean canAccess(String zygoteName, String user) {
        return true;
    }

    @Override
    public boolean canAccess(Task task, String user) {
        return true;
    }

    @Override
    public String userForTask(Task task) {
        try (Session session = storage.getSessionFactory().openSession()) {
            TaskModel taskModel = session.find(TaskModel.class, task.tid());
            return taskModel.getOwner().getUserId();
        }
    }

    @Override
    public void registerOperation(String zygoteName, String userId, Lzy.PublishRequest.VisibilityScope scope) {
    }

    @Override
    public String registerTask(String uid, Task task) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            final String token = UUID.randomUUID().toString();
            try {
                TaskModel taskModel = new TaskModel(task.tid(), token, new UserModel(uid));
                session.save(taskModel);
                tx.commit();
            }
            catch (Exception e){
                tx.rollback();
                throw e;
            }
            return token;
        }
    }

    @Override
    public boolean hasPermission(String uid, String permission) {
        try (Session session = storage.getSessionFactory().openSession()) {
            UserModel user = session.find(UserModel.class, uid);
            if (user == null){
                return false;
            }
            List<?> permissions = session.createSQLQuery(
                 "SELECT(permissions.*)\n" +
                    "FROM users\n" +
                    "JOIN role_to_user on users.user_id = role_to_user.user_id\n" +
                    "JOIN permission_to_role on role_to_user.role_id = permission_to_role.role_id\n" +
                    "JOIN permissions on permissions.name = permission_to_role.permission_id\n" +
                    "WHERE users.user_id = :userId"
            )
                    .setParameter("userId", user.getUserId())
                    .addEntity(PermissionModel.class)
                    .list();
            return permissions.contains(new PermissionModel(permission));
        }
    }

    @Override
    public boolean hasPermission(String uid, Permissions permission) {
        return hasPermission(uid, permission.name);
    }

    @Override
    public boolean checkBackOfficeSession(UUID sessionId, String userId) {
        try (Session session = storage.getSessionFactory().openSession()) {
            BackofficeSessionModel sessionModel = session.find(BackofficeSessionModel.class, sessionId);
            return Objects.equals(sessionModel.getOwner().getUserId(), userId);
        }
        catch (Exception e){
            return false;
        }
    }

    private boolean isUserTokenSigned(String userId, String token, String tokenSign) {
        LOG.info("isUserTokenSigned " + userId);
        try (Session session = storage.getSessionFactory().openSession()) {
            final UserModel user = session.find(UserModel.class, userId);
            if (user == null) {
                LOG.error("User " + userId + " not found in database");
                return false;
            }

            Security.addProvider(new BouncyCastleProvider());
            for (TokenModel userToken: user.getTokens()) {
                try (StringReader keyReader = new StringReader(userToken.getValue())) {
                    if (checkToken(keyReader, token, tokenSign)) {
                        LOG.info("Successfully checked user token " + userId);
                        return true;
                    }
                } catch (Exception e) {
                    LOG.error(e);
                }
            }
            return false;
        }
    }
}
