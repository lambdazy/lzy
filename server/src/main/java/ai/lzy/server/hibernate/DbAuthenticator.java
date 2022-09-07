package ai.lzy.server.hibernate;

import ai.lzy.server.Authenticator;
import ai.lzy.server.configs.StorageConfigs;
import ai.lzy.server.hibernate.models.BackofficeSessionModel;
import ai.lzy.server.hibernate.models.PermissionModel;
import ai.lzy.server.hibernate.models.PublicKeyModel;
import ai.lzy.server.hibernate.models.ServantModel;
import ai.lzy.server.hibernate.models.TaskModel;
import ai.lzy.server.hibernate.models.UserModel;
import ai.lzy.server.task.Task;
import ai.lzy.util.auth.credentials.JwtUtils;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ai.lzy.model.utils.Permissions;
import ai.lzy.v1.Lzy;

import java.io.StringReader;
import java.security.Security;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Singleton
@Requires(property = "database.enabled", value = "true", defaultValue = "false")
public class DbAuthenticator implements Authenticator {
    private static final Logger LOG = LogManager.getLogger(DbAuthenticator.class);

    @Inject
    private DbStorage storage;

    @Inject
    private StorageConfigs storageConfigs;

    @Override
    public boolean checkUser(String userId, String token) {
        LOG.info("checkUser userId=" + userId);
        return isUserTokenSigned(userId, token);
    }

    @Override
    public boolean checkTask(String tid, String servantId, String servantToken) {
        LOG.info("checkTask tid=" + tid);
        try (Session session = storage.getSessionFactory().openSession()) {

            if (tid == null) {
                ServantModel servant = session.find(ServantModel.class, servantId);
                if (servant == null) {
                    return false;
                }
                return servant.token().equals(servantToken);
            }

            TaskModel taskModel = session.find(TaskModel.class, tid);
            if (taskModel == null) {
                return false;
            }
            return taskModel.servant().servantId().equals(servantId)
                    && taskModel.servant().token().equals(servantToken);
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
        if (task == null) {
            return null;
        }
        LOG.info("User for task tid=" + task.tid());
        try (Session session = storage.getSessionFactory().openSession()) {
            TaskModel taskModel = session.find(TaskModel.class, task.tid());
            if (taskModel == null) {
                return null;
            }
            return taskModel.getOwner().getUserId();
        }
    }

    @Override
    public void registerOperation(String zygoteName, String userId, Lzy.PublishRequest.VisibilityScope scope) {
    }

    @Override
    public void registerTask(String uid, Task task, String servantId) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            UserModel user = session.find(UserModel.class, uid);
            ServantModel servant = session.find(ServantModel.class, servantId);
            try {
                TaskModel taskModel = new TaskModel(task.tid(), user, servant);
                session.save(taskModel);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    @Override
    public String registerServant(String servantId) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                ServantModel servant = new ServantModel(servantId, "servant_token_" + UUID.randomUUID());
                session.save(servant);
                tx.commit();
                return servant.token();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    @Override
    public boolean hasPermission(String uid, String permission) {
        try (Session session = storage.getSessionFactory().openSession()) {
            UserModel user = session.find(UserModel.class, uid);
            if (user == null) {
                return false;
            }
            List<?> permissions = session.createSQLQuery(
                    "SELECT(permissions.*)\n"
                        + "FROM users\n"
                        + "JOIN role_to_user on users.user_id = role_to_user.user_id\n"
                        + "JOIN permission_to_role on role_to_user.role_id = permission_to_role.role_id\n"
                        + "JOIN permissions on permissions.name = permission_to_role.permission_id\n"
                        + "WHERE users.user_id = :userId"
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
    public boolean checkBackOfficeSession(String sessionId, String userId) {
        try (Session session = storage.getSessionFactory().openSession()) {
            BackofficeSessionModel sessionModel = session.find(BackofficeSessionModel.class, sessionId);
            return Objects.equals(sessionModel.getOwner().getUserId(), userId);
        } catch (Exception e) {
            LOG.error("Exception while getting backoffice session", e);
            return false;
        }
    }

    @Override
    public boolean canAccessBucket(String uid, String bucket) {
        return bucket.equals(bucketForUser(uid));
    }

    @Override
    public String bucketForUser(String uid) {
        if (!storageConfigs.isSeparated()) {
            return storageConfigs.getBucket();
        }
        try (Session session = storage.getSessionFactory().openSession()) {
            UserModel user = session.find(UserModel.class, uid);
            if (user == null) {
                return null;
            }
            return user.getBucket();
        }
    }

    private boolean isUserTokenSigned(String userId, String token) {
        LOG.info("isUserTokenSigned " + userId);
        try (Session session = storage.getSessionFactory().openSession()) {
            final UserModel user = session.find(UserModel.class, userId);
            if (user == null) {
                LOG.error("User " + userId + " not found in database");
                return false;
            }
            if (user.getAccessType() != UserVerificationType.ACCESS_ALLOWED) {
                LOG.error("User " + userId + " has no access to Lzy::" + user.getAccessType());
                return false;
            }

            Security.addProvider(new BouncyCastleProvider());
            for (PublicKeyModel userToken : user.getPublicKeys()) {
                try (StringReader keyReader = new StringReader(userToken.getValue())) {
                    if (JwtUtils.legacyCheckJWT(keyReader, token, userId)) {
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
