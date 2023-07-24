package ai.lzy.service.test;

import ai.lzy.longrunning.dao.OperationDaoDecorator;
import ai.lzy.service.App;
import ai.lzy.test.context.LzyServiceContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static ai.lzy.service.test.LzyServiceContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class LzyServiceContextImpl implements LzyServiceContext {
    public static final String ENV_NAME = "common_lzy_service_context";

    private ApplicationContext micronautContext;
    private App lzyServiceApp;

    @Override
    public void setUp(Path configPath, Map<String, Object> runtimeConfig, String... environments) throws IOException {
        try (var file = new FileInputStream(configPath.toFile())) {
            var actualConfig = new YamlPropertySourceLoader().read("lzy-service", file);
            actualConfig.putAll(runtimeConfig);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        lzyServiceApp = micronautContext.getBean(App.class);
        lzyServiceApp.start(true);
    }

    @Override
    public void tearDown() {
        lzyServiceApp.shutdown(true);
        micronautContext.close();
    }

    public OperationDaoDecorator operationsDao() {
        return micronautContext.getBean(OperationDaoDecorator.class);
    }
}
