package ai.lzy.iam.test;

import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.impl.DbSubjectService;
import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.annotation.Nullable;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static ai.lzy.iam.BeanFactory.TEST_ENV_NAME;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class BaseTestWithIam {
    private ApplicationContext iamCtx;
    private LzyIAM iamApp;
    private int port;

    private ManagedChannel grpcChannel;
    private SubjectServiceGrpcClient grpcClient;

    public void before() throws IOException, InterruptedException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var iamProps = new YamlPropertySourceLoader().read("iam",
            new FileInputStream("../iam/src/main/resources/application-test.yml"));
        iamProps.putAll(overrides);
        iamCtx = ApplicationContext.run(PropertySource.of(iamProps), TEST_ENV_NAME);

        var config = iamCtx.getBean(ServiceConfig.class);
        port = GrpcUtils.rollPort();
        config.setServerPort(port);

        iamApp = iamCtx.getBean(LzyIAM.class);
        iamApp.start();

        var internalUserCredentials = getClientConfig().createRenewableToken();
        grpcChannel = newGrpcChannel(
            "localhost:" + getPort(), LzySubjectServiceGrpc.SERVICE_NAME, LzyAccessBindingServiceGrpc.SERVICE_NAME
        );
        grpcClient = new SubjectServiceGrpcClient("TestClient", grpcChannel, internalUserCredentials::get);
    }

    public void after() {
        grpcChannel.shutdown();
        iamApp.close();
        iamCtx.close();
    }

    public int getPort() {
        return this.port;
    }

    public String getAddress() {
        return "localhost:" + getPort();
    }

    public SubjectServiceGrpcClient iamSubjectsClient() {
        return grpcClient;
    }

    public LzySubjectServiceDecorator subjectsService() {
        return iamCtx.getBean(LzySubjectServiceDecorator.class);
    }

    @Nullable
    public Subject getSubject(AuthProvider provider, String providerSubjectId, SubjectType type) {
        try {
            var subjectService = iamCtx.getBean(DbSubjectService.class);
            return subjectService.getSubjectForTests(provider, providerSubjectId, type);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public IamClientConfiguration getClientConfig() {
        var internalUserConfig = iamCtx.getBean(InternalUserConfig.class);
        var iamClientConfiguration = new IamClientConfiguration();

        iamClientConfiguration.setAddress("localhost:" + port);
        iamClientConfiguration.setInternalUserName(internalUserConfig.userName());
        iamClientConfiguration.setInternalUserPrivateKey(internalUserConfig.credentialPrivateKey());

        return iamClientConfiguration;
    }

    @Nullable
    public List<SubjectCredentials> listCredentials(String subjectId) {
        var subjectService = iamCtx.getBean(DbSubjectService.class);
        return subjectService.listCredentials(subjectId);
    }
}
