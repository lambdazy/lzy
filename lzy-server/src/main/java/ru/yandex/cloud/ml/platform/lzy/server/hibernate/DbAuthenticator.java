package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
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
@Requires(classes = Storage.class)
public class DbAuthenticator implements Authenticator {
    private final Map<String, String> taskTokens = new HashMap<>();
    private final Map<String, String> owners = new HashMap<>();
    @Inject
    private Storage storage;

    @Override
    public boolean checkUser(String userId, String token) {
        String[] tokenParts = token.split("\\.");
        return isUserTokenSigned(userId, tokenParts[0], tokenParts[1]);
    }

    @Override
    public boolean checkTask(String tid, String token) {
        return true;
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

            final byte[] publicKeyPEM = Base64.getDecoder().decode(
                    user.getPublicToken().replaceAll("-----[^-]*-----\\n", "")
                            .replaceAll("\\R", "")
            );
            try {
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
