package ai.lzy.iam.grpc.client;

import ai.lzy.iam.BaseAuthServiceApiTest;
import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.model.db.test.DatabaseCleaner;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class AuthenticateServiceGrpcClientTest extends BaseAuthServiceApiTest {
    private static final Logger LOG = LogManager.getLogger(AuthenticateServiceGrpcClientTest.class);

    private ApplicationContext ctx;
    private AuthenticateServiceGrpcClient authenticateService;
    private SubjectServiceGrpcClient subjectService;
    private LzyIAM lzyIAM;

    @Before
    public void setUp() throws IOException {
        ctx = ApplicationContext.run();
        lzyIAM = new LzyIAM(ctx);
        lzyIAM.start();
        ServiceConfig iamConfig = ctx.getBean(ServiceConfig.class);
        InternalUserConfig internalUserConfig = ctx.getBean(InternalUserConfig.class);
        GrpcConfig grpcConfig = GrpcConfig.from("localhost:" + iamConfig.getServerPort());
        subjectService = new SubjectServiceGrpcClient(
                grpcConfig,
                () -> JwtUtils.credentials(internalUserConfig.userName(), internalUserConfig.credentialPrivateKey())
        );
        authenticateService = new AuthenticateServiceGrpcClient(grpcConfig);
    }

    @After
    public void tearDown() {
        lzyIAM.close();
        ctx.close();
    }

    @Override
    protected Subject subject(String id) {
        return subjectService.getSubject(id);
    }

    @Override
    protected void createSubject(String id, String name, String value, SubjectType subjectType) {
        subjectService.createSubject(id, name, value, subjectType);
    }

    @Override
    protected void removeSubject(Subject subject) {
        subjectService.removeSubject(subject);
    }

    @Override
    protected void addCredentials(Subject subject, String name, String value, String type) {
        subjectService.addCredentials(subject, name, value, type);
    }

    @Override
    protected void removeCredentials(Subject subject, String name) {
        subjectService.removeCredentials(subject, name);
    }

    @Override
    protected void authenticate(Credentials credentials) {
        authenticateService.authenticate(credentials);
    }
}
