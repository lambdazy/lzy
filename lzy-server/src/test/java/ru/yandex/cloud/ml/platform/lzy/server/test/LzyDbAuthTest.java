package ru.yandex.cloud.ml.platform.lzy.server.test;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.Storage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.TaskModel;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.io.IOException;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

@MicronautTest
@Property(name="authenticator", value="DbAuthenticator")
public class LzyDbAuthTest {
    User[] users = null;
    @Inject
    private DbStorage storage;
    private InProcessServer<LzyServer.Impl> inprocessServer;
    private io.grpc.ManagedChannel channel;
    private LzyServerGrpc.LzyServerBlockingStub blockingStub;

    static class User{
        public String publicKey;
        public String privateKey;
        public String userId;
        public String token = null;

        public User(String userId) throws NoSuchAlgorithmException {
            this.userId = userId;
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(1024);
            KeyPair kp = kpg.generateKeyPair();
            publicKey = new String(Base64.getEncoder().encode(kp.getPublic().getEncoded()));
            privateKey = new String(Base64.getEncoder().encode(kp.getPrivate().getEncoded()));
        }

        public String getToken(){
            if (token != null)
                return token;
            UUID token = UUID.randomUUID();
            try {
                String signedToken = signToken(token, privateKey);
                this.token = token + "." + signedToken;
                return this.token;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        public UserModel getUserModel(){
            return new UserModel(userId, getToken());
        }

        private String signToken(UUID terminalToken, String privateKey) throws
                InvalidKeySpecException,
                NoSuchAlgorithmException,
                InvalidKeyException,
                SignatureException {
            java.security.Security.addProvider(
                    new org.bouncycastle.jce.provider.BouncyCastleProvider()
            );

            final String tokenSignature;
            final byte[] privKeyPEM = Base64.getDecoder().decode(
                    privateKey
            );

            final PrivateKey rsaKey = KeyFactory.getInstance("RSA")
                    .generatePrivate(new PKCS8EncodedKeySpec(privKeyPEM));
            final Signature sign = Signature.getInstance("SHA1withRSA");
            sign.initSign(rsaKey);
            sign.update(terminalToken.toString().getBytes());
            tokenSignature = new String(Base64.getEncoder().encode(sign.sign()));
            return tokenSignature;
        }
    }

    @Before
    public void before() throws IOException, InstantiationException, IllegalAccessException {
        try {
            users = new User[]{
                    new User("user1"), new User("user2"), new User("user3")
            };
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try(Session session =  storage.getSessionFactory().openSession()){
            Transaction tx = session.beginTransaction();
            for (User user: users) {
                session.persist(user.getUserModel());
            }
            tx.commit();
        }
        inprocessServer = new InProcessServer<>(LzyServer.Impl.class);
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
    }


    @Test
    public void testCanAuth(){
        User user = users[0];
        Operations.ZygoteList list = blockingStub.zygotes(
            IAM.Auth.newBuilder()
                .setUser(
                    IAM.UserCredentials.newBuilder()
                            .setUserId(user.userId)
                            .setToken(user.getToken())
                            .build())
                .build()
        );
        assert list != null;
    }
}
