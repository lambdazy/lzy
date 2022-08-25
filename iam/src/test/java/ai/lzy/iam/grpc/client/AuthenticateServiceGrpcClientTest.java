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
    // TODO: delete entities after every test with services interfaces instead of DB
    private IamDataSource storage;

    @Before
    public void setUp() throws IOException {
        System.out.println("HEREEEEEEEEEEEEEEEEEE");
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
        storage = ctx.getBean(IamDataSource.class);
    }

    @After
    public void tearDown() {
        lzyIAM.close();
        ctx.close();
        DatabaseCleaner.cleanup(storage);
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
    protected void addCredentials(Subject subject, String name, String value, String type) {
        subjectService.addCredentials(subject, name, value, type);
    }

    @Override
    protected void authenticate(Credentials credentials) {
        authenticateService.authenticate(credentials);
    }
}
