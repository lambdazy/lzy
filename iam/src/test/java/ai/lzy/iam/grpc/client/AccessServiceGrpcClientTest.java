package ai.lzy.iam.grpc.client;

import ai.lzy.iam.BaseAccessServiceApiTest;
import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public class AccessServiceGrpcClientTest extends BaseAccessServiceApiTest {

    private ApplicationContext ctx;
    private SubjectServiceGrpcClient subjectService;
    private AccessBindingServiceGrpcClient accessBindingService;
    private AccessServiceGrpcClient accessService;
    private LzyIAM lzyIAM;

    @Before
    public void setUp() throws IOException {
        ctx = ApplicationContext.run();
        lzyIAM = new LzyIAM(ctx);
        lzyIAM.start();
        ServiceConfig iamConfig = ctx.getBean(ServiceConfig.class);
        InternalUserConfig internalUserConfig = ctx.getBean(InternalUserConfig.class);
        GrpcConfig grpcConfig = GrpcConfig.from("localhost:" + iamConfig.getServerPort());
        JwtCredentials credentials = JwtUtils.credentials(
                internalUserConfig.userName(),
                internalUserConfig.credentialPrivateKey()
        );
        subjectService = new SubjectServiceGrpcClient(
                grpcConfig,
                () -> credentials
        );
        accessBindingService = new AccessBindingServiceGrpcClient(
                grpcConfig,
                () -> credentials
        );
        accessService = new AccessServiceGrpcClient(
                grpcConfig,
                () -> credentials
        );
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
    protected void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) {
        accessBindingService.setAccessBindings(resource, accessBinding);
    }

    @Override
    protected Stream<AccessBinding> listAccessBindings(AuthResource resource) {
        return accessBindingService.listAccessBindings(resource);
    }

    @Override
    protected void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBinding) {
        accessBindingService.updateAccessBindings(resource, accessBinding);
    }

    @Override
    protected boolean hasResourcePermission(Subject subject, AuthResource resource, AuthPermission permission) {
        return accessService.hasResourcePermission(subject, resource, permission);
    }
}
