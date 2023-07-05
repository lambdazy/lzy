package ai.lzy.iam.test;

import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.test.context.IamContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static ai.lzy.iam.test.IamContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class IamContextImpl implements IamContext {
    public static final String ENV_NAME = "common_iam_test";

    private ApplicationContext micronautContext;
    private LzyIAM iamApp;

    @Override
    public void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws IOException {
        try (var file = new FileInputStream(config.toFile())) {
            var actualConfig = new YamlPropertySourceLoader().read("iam", file);
            actualConfig.putAll(runtimeConfig);
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
}
