package ai.lzy.iam.grpc.client;

import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;

public class ClientAuthTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    ApplicationContext ctx;
    LzyIAM lzyIAM;
    SubjectServiceGrpcClient subjectClient;
    AuthenticateServiceGrpcClient authClient;

    @Before
    public void setUp() throws IOException {
        ctx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("iam", db.getConnectionInfo()));

        lzyIAM = new LzyIAM(ctx);
        lzyIAM.start();

        var internalUserConfig = ctx.getBean(InternalUserConfig.class);
        var internalUserCredentials = JwtUtils.credentials(
            internalUserConfig.userName(), AuthProvider.INTERNAL.name(), internalUserConfig.credentialPrivateKey());

        subjectClient = new SubjectServiceGrpcClient(getIamAddress(), () -> internalUserCredentials);
        authClient = new AuthenticateServiceGrpcClient(getIamAddress());
    }

    @After
    public void shutdown() {
        DatabaseTestUtils.cleanup(ctx.getBean(IamDataSource.class));

        lzyIAM.close();
        ctx.close();
    }

    @Test
    public void testAuth() throws IOException, InterruptedException {
        var keys = RsaUtils.generateRsaKeys();
        var login = "user1";

        var subject = subjectClient.createSubject(AuthProvider.GITHUB, login, SubjectType.USER);
        Assert.assertEquals(SubjectType.USER, subject.type());

        subjectClient.addCredentials(
            subject, "main", Files.readString(keys.publicKeyPath()), CredentialsType.PUBLIC_KEY);

        var subject2 = authClient.authenticate(
            JwtUtils.credentials(login, AuthProvider.GITHUB.name(), Files.readString(keys.privateKeyPath())));
        Assert.assertEquals(subject, subject2);

        subjectClient.removeCredentials(subject, "main");
        try {
            authClient.authenticate(
                JwtUtils.credentials(login, AuthProvider.GITHUB.name(), Files.readString(keys.privateKeyPath())));
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
