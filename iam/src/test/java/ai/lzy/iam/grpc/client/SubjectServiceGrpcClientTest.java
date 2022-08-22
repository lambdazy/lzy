package ai.lzy.iam.grpc.client;

import ai.lzy.iam.BaseSubjectServiceApiTest;
import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.impl.DbSubjectService;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtCredentials;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static ai.lzy.iam.utils.CredentialsHelper.buildJWT;

public class SubjectServiceGrpcClientTest extends BaseSubjectServiceApiTest {
    private static final Logger LOG = LogManager.getLogger(SubjectServiceGrpcClientTest.class);

    ApplicationContext ctx;
    SubjectServiceGrpcClient subjectClient;
    DbSubjectService dbSubjectService;  // for getting credentials and subjects from DB
    LzyIAM lzyIAM;

    @Before
    public void setUp() throws IOException {
        ctx = ApplicationContext.run();
        InternalUserConfig internalUserConfig = ctx.getBean(InternalUserConfig.class);
        Credentials credentials;
        try (final Reader reader = new StringReader(internalUserConfig.credentialPrivateKey())) {
            credentials = new JwtCredentials(buildJWT(internalUserConfig.userName(), reader));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Cannot build credentials");
        }
        lzyIAM = new LzyIAM(ctx);
        lzyIAM.start();
        ServiceConfig iamConfig = ctx.getBean(ServiceConfig.class);
        subjectClient = new SubjectServiceGrpcClient(
            GrpcConfig.from("localhost:" + iamConfig.getServerPort()),
            () -> credentials
        );
        dbSubjectService = ctx.getBean(DbSubjectService.class);
    }

    @After
    public void shutdown() {
        lzyIAM.close();
    }

    @Override
    protected Subject subject(String id) {
        return dbSubjectService.subject(id);
    }

    @Override
    protected void createSubject(String id, SubjectType subjectType) {
        subjectClient.createSubject(id, "provider", "providerID");
    }

    @Override
    protected void removeSubject(Subject subject) {
        subjectClient.removeSubject(subject);
    }

    @Override
    protected SubjectCredentials credentials(Subject subject, String id) {
        return dbSubjectService.credentials(subject, id);
    }

    @Override
    protected void addCredentials(Subject subject, String name) {
        subjectClient.addCredentials(subject, name, "Value", "Type");
    }

    @Override
    protected void removeCredentials(Subject subject, String name) {
        subjectClient.removeCredentials(subject, name);
    }
}
