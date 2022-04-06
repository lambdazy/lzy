package ru.yandex.cloud.ml.platform.lzy.server;

import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.micronaut.context.ApplicationContext;
import java.io.IOException;
import java.io.StringReader;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.utils.JwtCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.UserVerificationType;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.PublicKeyModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;


public class LzyDbAuthTest {
    User[] users = null;
    private InProcessServer<LzyServer.Impl> inprocessServer;
    private io.grpc.ManagedChannel channel;
    private LzyServerGrpc.LzyServerBlockingStub blockingStub;
    private ApplicationContext ctx;
    private DbStorage storage;

    private void generateContext() {
        ctx = ApplicationContext.run(Map.of(
            "authenticator", "DbAuthenticator",
            "server.server-uri", "https://lzy.server.com:8888",
            "server.whiteboard-url", "",
            "database.enabled", true
        ));
    }

    @Before
    public void before() throws IOException {
        generateContext();
        storage = ctx.getBean(DbStorage.class);

        try {
            users = new User[] {
                new User("user1"), new User("user2"), new User("user3")
            };
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            for (User user : users) {
                session.persist(user.getUserModel());
                session.persist(new PublicKeyModel("main",
                    "-----BEGIN PUBLIC KEY-----\n" + user.publicKey + "\n-----END PUBLIC KEY-----", user.userId));
            }
            tx.commit();
        }

        LzyServer.Impl impl = ctx.getBean(LzyServer.Impl.class);

        inprocessServer = new InProcessServer<>(impl);
        inprocessServer.start();
        channel = InProcessChannelBuilder
            .forName("test")
            .directExecutor()
            .usePlaintext()
            .build();
        blockingStub = LzyServerGrpc.newBlockingStub(channel);
    }

    @After
    public void after() throws InterruptedException {
        channel.shutdownNow();
        inprocessServer.stop();
        inprocessServer.blockUntilShutdown();
        ctx.stop();
    }

    @Test
    public void testCanAuth() {
        User user = users[0];
        Operations.ZygoteList list = blockingStub.zygotes(
            IAM.Auth.newBuilder()
                .setUser(
                    IAM.UserCredentials.newBuilder()
                        .setUserId(user.userId)
                        .setToken(user.getToken(user.userId))
                        .build())
                .build()
        );
        assert list != null;
    }

    @Test(expected = StatusRuntimeException.class)
    public void testWrongAuth() throws NoSuchAlgorithmException {
        User user = new User("some_wrong_user");

        Operations.ZygoteList list = blockingStub.zygotes(
            IAM.Auth.newBuilder()
                .setUser(
                    IAM.UserCredentials.newBuilder()
                        .setUserId(user.userId)
                        .setToken(user.getToken(user.userId))
                        .build())
                .build()
        );
        assert list != null;
    }

    @Test
    public void testTaskToken() {
//        UUID taskUUID = UUID.randomUUID();
//        String taskToken = UUID.randomUUID().toString();
//
//        try (Session session = storage.getSessionFactory().openSession()) {
//            Transaction tx = session.beginTransaction();
//            session.save(new TaskModel(taskUUID, taskToken, users[0].getUserModel()));
//            tx.commit();
//        }
//
//        Authenticator authenticator = ctx.getBean(Authenticator.class);
//        assert authenticator.checkTask(taskUUID.toString(), taskToken);
    }

    static class User {
        public String publicKey;
        public String privateKey;
        public String userId;
        public String token = null;

        public User(String userId) throws NoSuchAlgorithmException {
            this.userId = userId;
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            KeyPair kp = kpg.generateKeyPair();
            publicKey = new String(Base64.getEncoder().encode(kp.getPublic().getEncoded()));
            privateKey = new String(Base64.getEncoder().encode(kp.getPrivate().getEncoded()));
        }

        public String getToken(String uid) {
            if (token != null) {
                return token;
            }
            try (StringReader reader = new StringReader(
                "-----BEGIN RSA PRIVATE KEY-----\n" + privateKey + "\n-----END RSA PRIVATE KEY-----")) {
                this.token = JwtCredentials.buildJWT(uid, reader);
                return this.token;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public UserModel getUserModel() {
            return new UserModel(userId, userId.toLowerCase(Locale.ROOT), UserVerificationType.ACCESS_ALLOWED);
        }
    }
}
