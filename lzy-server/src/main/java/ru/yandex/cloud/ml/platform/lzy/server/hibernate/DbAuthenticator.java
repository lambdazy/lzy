package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.TaskModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Security;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


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
                TaskModel taskModel = new TaskModel(task.tid(), token, new UserModel(uid, null));
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

    private boolean isUserTokenSigned(String userId, String token, String tokenSign)
    {
        try (Session session = storage.getSessionFactory().openSession()) {
            final UserModel user;
            user = session.find(UserModel.class, userId);
            if (user == null) {
                return false;
            }

            Security.addProvider(
                    new org.bouncycastle.jce.provider.BouncyCastleProvider()
            );

            try {
                final byte[] publicKeyPEM = Base64.getDecoder().decode(
                        user.getPublicToken().replaceAll("-----[^-]*-----\\n", "")
                                .replaceAll("\\R", "")
                );
                final PublicKey rsaKey = KeyFactory.getInstance("RSA")
                        .generatePublic(new X509EncodedKeySpec(publicKeyPEM));

                final Signature sign = Signature.getInstance("SHA1withRSA");
                sign.initVerify(rsaKey);
                sign.update(token.getBytes());

                return sign.verify(Base64.getDecoder().decode(tokenSign));
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
