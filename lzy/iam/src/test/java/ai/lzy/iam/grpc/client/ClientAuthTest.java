package ai.lzy.iam.grpc.client;

import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class ClientAuthTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    ApplicationContext ctx;
    LzyIAM lzyIAM;
    ManagedChannel iamChannel;
    SubjectServiceGrpcClient subjectClient;
    AuthenticateServiceGrpcClient authClient;

    @Before
    public void setUp() throws IOException {
        ctx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("iam", db.getConnectionInfo()));

        lzyIAM = ctx.getBean(LzyIAM.class);
        lzyIAM.start();

        var internalUserConfig = ctx.getBean(InternalUserConfig.class);
        var internalUserCredentials = JwtUtils.credentials(internalUserConfig.userName(), AuthProvider.INTERNAL.name(),
            Date.from(Instant.now()), JwtUtils.afterDays(1), internalUserConfig.credentialPrivateKey());

        iamChannel = newGrpcChannel(getIamAddress().host(), getIamAddress().port(), LzySubjectServiceGrpc.SERVICE_NAME);
        subjectClient = new SubjectServiceGrpcClient("TestClient", iamChannel, () -> internalUserCredentials);
        authClient = new AuthenticateServiceGrpcClient("TestClient", iamChannel);
    }

    @After
    public void shutdown() {
        ctx.getBean(IamDataSource.class).setOnClose(DatabaseTestUtils::cleanup);

        iamChannel.shutdown();
        lzyIAM.close();
        ctx.close();
    }

    @Test
    public void testAuth() throws IOException, InterruptedException {
        var keys = RsaUtils.generateRsaKeys();
        var login = "user1";

        var subject = subjectClient.createSubject(AuthProvider.GITHUB, login, SubjectType.USER,
            new SubjectCredentials("main", keys.publicKey(), CredentialsType.PUBLIC_KEY));
        Assert.assertEquals(SubjectType.USER, subject.type());

        var subject2 = authClient.authenticate(
            JwtUtils.credentials(login, AuthProvider.GITHUB.name(), Date.from(Instant.now()), JwtUtils.afterDays(1),
                keys.privateKey()));
        Assert.assertEquals(subject, subject2);

        subjectClient.removeCredentials(subject, "main");
        try {
            authClient.authenticate(
                JwtUtils.credentials(login, AuthProvider.GITHUB.name(), Date.from(Instant.now()), JwtUtils.afterDays(1),
                    keys.privateKey()));
            Assert.fail();
        } catch (AuthPermissionDeniedException e) {
            // ignored
        }
    }

    private GrpcConfig getIamAddress() {
        int port = ctx.getBean(ServiceConfig.class).getServerPort();
        return GrpcConfig.from("localhost:" + port);
    }
}
