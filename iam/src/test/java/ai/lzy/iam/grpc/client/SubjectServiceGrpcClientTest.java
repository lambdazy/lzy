package ai.lzy.iam.grpc.client;

import ai.lzy.iam.BaseSubjectServiceApiTest;
import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.io.IOException;
import java.util.NoSuchElementException;

public class SubjectServiceGrpcClientTest extends BaseSubjectServiceApiTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    ApplicationContext ctx;
    SubjectServiceGrpcClient subjectClient;
    LzyIAM lzyIAM;

    @Before
    public void setUp() throws IOException {
        ctx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("iam", db.getConnectionInfo()));

        InternalUserConfig internalUserConfig = ctx.getBean(InternalUserConfig.class);
        Credentials credentials = JwtUtils.credentials(
            internalUserConfig.userName(),
            AuthProvider.INTERNAL.name(),
            internalUserConfig.credentialPrivateKey()
        );
        lzyIAM = new LzyIAM(ctx);
        lzyIAM.start();
        ServiceConfig iamConfig = ctx.getBean(ServiceConfig.class);
        subjectClient = new SubjectServiceGrpcClient(
            GrpcConfig.from("localhost:" + iamConfig.getServerPort()),
            () -> credentials
        );
    }

    @After
    public void shutdown() {
        DatabaseTestUtils.cleanup(ctx.getBean(IamDataSource.class));

        lzyIAM.close();
        ctx.close();
    }

    @Test(expected = AuthInternalException.class)
    public void createSubjectUserWithInternalAuthProvider() {
        subjectClient.createSubject(AuthProvider.INTERNAL, "Superman", SubjectType.USER);
        Assert.fail();
    }

    public void createSubjectServantWithInternalAuthProvider() {
        var subject = subjectClient.createSubject(AuthProvider.INTERNAL, "Superman", SubjectType.SERVANT);
        Assert.assertEquals(SubjectType.SERVANT, subject.type());
    }

    @Override
    protected Subject subject(String id) {
        return subjectClient.getSubject(id);
    }

    @Override
    protected Subject createSubject(String name, SubjectType subjectType) {
        return subjectClient.createSubject(AuthProvider.GITHUB, name, subjectType);
    }

    @Override
    protected void removeSubject(Subject subject) {
        subjectClient.removeSubject(subject);
    }

    @Override
    protected SubjectCredentials credentials(Subject subject, String name) throws NoSuchElementException {
        return subjectClient.listCredentials(subject)
                .stream()
                .filter(c -> name.equals(c.name()))
                .findAny()
                .orElseThrow();
    }

    @Override
    protected void addCredentials(Subject subject, String name) {
        subjectClient.addCredentials(subject, name, "Value", CredentialsType.PUBLIC_KEY);
    }

    @Override
    protected void removeCredentials(Subject subject, String name) {
        subjectClient.removeCredentials(subject, name);
    }
}
