package ai.lzy.iam.test;

import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.InternalUserInserter;
import ai.lzy.iam.storage.impl.DbSubjectService;
import ai.lzy.test.context.IamContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static ai.lzy.iam.test.IamContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class IamContextImpl implements IamContext {
    public static final String ENV_NAME = "common_iam_test";

    private ApplicationContext micronautContext;
    private LzyIAM iamApp;

    @Override
    public void setUp(String baseConfigPath, Map<String, Object> configOverrides, String... environments)
        throws IOException
    {
        try (var file = new FileInputStream(Objects.requireNonNull(baseConfigPath))) {
            var actualConfig = new YamlPropertySourceLoader().read("iam", file);
            actualConfig.putAll(configOverrides);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        iamApp = micronautContext.getBean(LzyIAM.class);
        iamApp.start();
    }

    @Override
    public void tearDown() {
        iamApp.close();
        micronautContext.close();
    }

    public LzySubjectServiceDecorator subjectsService() {
        return micronautContext.getBean(LzySubjectServiceDecorator.class);
    }

    public IamClientConfiguration clientConfig() {
        var internalUserConfig = micronautContext.getBean(InternalUserConfig.class);
        var config = micronautContext.getBean(ServiceConfig.class);
        var iamClientConfiguration = new IamClientConfiguration();

        iamClientConfiguration.setAddress("localhost:" + config.getServerPort());
        iamClientConfiguration.setInternalUserName(internalUserConfig.userName());
        iamClientConfiguration.setInternalUserPrivateKey(internalUserConfig.credentialPrivateKey());

        return iamClientConfiguration;
    }

    @Nullable
    public Subject getSubject(AuthProvider provider, String providerSubjectId, SubjectType type) {
        try {
            var subjectService = micronautContext.getBean(DbSubjectService.class);
            return subjectService.getSubjectForTests(provider, providerSubjectId, type);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    public List<SubjectCredentials> listCredentials(String subjectId) {
        var subjectService = micronautContext.getBean(DbSubjectService.class);
        return subjectService.listCredentials(subjectId);
    }

    public Subject createAdminSubject(String name, String publicKey) {
        try {
            return micronautContext.getBean(InternalUserInserter.class).addAdminUser(name, publicKey);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
