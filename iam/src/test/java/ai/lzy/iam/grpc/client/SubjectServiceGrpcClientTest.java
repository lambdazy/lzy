package ai.lzy.iam.grpc.client;

import ai.lzy.iam.BaseSubjectServiceApiTest;
import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.NoSuchElementException;

public class SubjectServiceGrpcClientTest extends BaseSubjectServiceApiTest {
    private static final Logger LOG = LogManager.getLogger(SubjectServiceGrpcClientTest.class);

    ApplicationContext ctx;
    SubjectServiceGrpcClient subjectClient;
    LzyIAM lzyIAM;

    @Before
    public void setUp() throws IOException {
        ctx = ApplicationContext.run();
        InternalUserConfig internalUserConfig = ctx.getBean(InternalUserConfig.class);
        Credentials credentials = JwtUtils.credentials(
            internalUserConfig.userName(),
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
        lzyIAM.close();
        ctx.close();
    }

    @Override
    protected Subject subject(String id) {
        return subjectClient.getSubject(id);
    }

    @Override
    protected void createSubject(String id, SubjectType subjectType) {
        subjectClient.createSubject(id, "provider", "providerID", subjectType);
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
        subjectClient.addCredentials(subject, name, "Value", "Type");
    }

    @Override
    protected void removeCredentials(Subject subject, String name) {
        subjectClient.removeCredentials(subject, name);
    }
}
