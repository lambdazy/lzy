package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.Permissions;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.*;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.io.FileReader;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Security;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;


@Singleton
@Requires(property = "authenticator", value = "DbAuthenticator")
public class DbAuthenticator implements Authenticator {
    private final Map<String, String> taskTokens = new HashMap<>();
    private final Map<String, String> owners = new HashMap<>();
    @Inject
    private DbStorage storage;

    @Override
    public boolean checkUser(String userId, String token) {
        String[] tokenParts = token.split("\\.");
        return isUserTokenSigned(userId, tokenParts[0], tokenParts[1]);
    }

    @Override
    public boolean checkTask(String tid, String token) {
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
    public boolean hasPermission(String uid, Permissions permission) {
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
            return permissions.contains(new PermissionModel(permission.name));
        }
    }

    private boolean isUserTokenSigned(String userId, String token, String tokenSign) {
        try (Session session = storage.getSessionFactory().openSession()) {
            final UserModel user;
            user = session.find(UserModel.class, userId);
            if (user == null) {
                return false;
            }

            Security.addProvider(
                    new org.bouncycastle.jce.provider.BouncyCastleProvider()
            );
            for (TokenModel user_token: user.getTokens()) {
                try (StringReader keyReader = new StringReader(user_token.getValue()); PemReader pemReader = new PemReader(keyReader)){
                    KeyFactory factory = KeyFactory.getInstance("RSA");
                    PemObject pemObject = pemReader.readPemObject();
                    byte[] content = pemObject.getContent();
                    X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(content);
                    PublicKey rsaKey = factory.generatePublic(pubKeySpec);

                    final Signature sign = Signature.getInstance("SHA1withRSA");
                    sign.initVerify(rsaKey);
                    sign.update(token.getBytes());

                    if (sign.verify(Base64.getDecoder().decode(tokenSign)))
                        return true;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return false;
        }
    }
}
