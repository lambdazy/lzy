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
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.exceptions.AuthInternalException;
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
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class SubjectServiceGrpcClientTest extends BaseSubjectServiceApiTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    ApplicationContext ctx;
    ManagedChannel iamChannel;
    SubjectServiceGrpcClient subjectClient;
    LzyIAM lzyIAM;

    @Before
    public void setUp() throws IOException {
        ctx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("iam", db.getConnectionInfo()));

        InternalUserConfig internalUserConfig = ctx.getBean(InternalUserConfig.class);
        Credentials credentials = JwtUtils.credentials(
            internalUserConfig.userName(),
            AuthProvider.INTERNAL.name(),
            Date.from(Instant.now()),
            JwtUtils.afterDays(1),
            internalUserConfig.credentialPrivateKey()
        );
        lzyIAM = ctx.getBean(LzyIAM.class);
        lzyIAM.start();
        ServiceConfig iamConfig = ctx.getBean(ServiceConfig.class);
        iamChannel = newGrpcChannel("localhost", iamConfig.getServerPort(), LzySubjectServiceGrpc.SERVICE_NAME);
        subjectClient = new SubjectServiceGrpcClient("TestClient", iamChannel, () -> credentials);
    }

    @After
    public void shutdown() {
        ctx.getBean(IamDataSource.class).setOnClose(DatabaseTestUtils::cleanup);

        iamChannel.shutdown();
        lzyIAM.close();
        ctx.close();
    }

    @Test(expected = AuthInternalException.class)
    public void createSubjectUserWithInternalAuthProvider() {
        subjectClient.createSubject(AuthProvider.INTERNAL, "Superman", SubjectType.USER);
        Assert.fail();
    }

    @Test
    public void createSubjectWorkerWithInternalAuthProvider() {
        var subject = subjectClient.createSubject(AuthProvider.INTERNAL, "Superman", SubjectType.WORKER);
        Assert.assertEquals(SubjectType.WORKER, subject.type());
    }

    @Test
    public void createSubjectWithCredentials() {
        var creds1 = new SubjectCredentials("first", "first value", CredentialsType.PUBLIC_KEY);
        var creds2 = new SubjectCredentials("second", "second value", CredentialsType.PUBLIC_KEY,
            Instant.now().plus(1, ChronoUnit.DAYS));

        var subject = subjectClient.createSubject(AuthProvider.INTERNAL, "Superman", SubjectType.WORKER,
            creds1, creds2);
        Assert.assertEquals(SubjectType.WORKER, subject.type());

        var creds = subjectClient.listCredentials(subject);
        Assert.assertEquals(2, creds.size());

        creds = creds.stream().sorted(Comparator.comparing(SubjectCredentials::name)).toList();
        Assert.assertEquals(creds1, creds.get(0));
        Assert.assertEquals(creds2, creds.get(1));
    }

    @Override
    protected Subject subject(String id) {
        return subjectClient.getSubject(id);
    }

    @Override
    protected Subject createSubject(String name, SubjectType subjectType) {
        return createSubject(name, subjectType, Collections.emptyList());
    }

    @Override
    protected Subject createSubject(String name, SubjectType subjectType, List<SubjectCredentials> credentials) {
        var authProvider = subjectType == SubjectType.WORKER ? AuthProvider.INTERNAL : AuthProvider.GITHUB;
        return createSubject(name, subjectType, authProvider, credentials);
    }

    @Override
    protected Subject createSubject(String name, SubjectType subjectType, AuthProvider authProvider,
                                    List<SubjectCredentials> credentials)
    {
        return subjectClient.createSubject(authProvider, name, subjectType,
            credentials.toArray(new SubjectCredentials[0]));
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
        subjectClient.addCredentials(subject, new SubjectCredentials(name, "Value", CredentialsType.PUBLIC_KEY));
    }

    @Override
    protected void removeCredentials(Subject subject, String name) {
        subjectClient.removeCredentials(subject, name);
    }
}
