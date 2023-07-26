package ai.lzy.storage.test;

import ai.lzy.storage.App;
import ai.lzy.test.context.StorageServiceContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static ai.lzy.storage.test.StorageContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class StorageContextImpl implements StorageServiceContext {
    public static final String ENV_NAME = "common_storage_context";

    private ApplicationContext micronautContext;
    private App storageApp;

    @Override
    public void setUp(String baseConfigPath, Map<String, Object> configOverrides, String... environments)
        throws IOException
    {
        try (var file = new FileInputStream(Objects.requireNonNull(baseConfigPath))) {
            var actualConfig = new YamlPropertySourceLoader().read("storage", file);
            actualConfig.putAll(configOverrides);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        storageApp = micronautContext.getBean(App.class);
        storageApp.start();
    }

    @Override
    public void tearDown() {
        storageApp.close(false);
        try {
            storageApp.awaitTermination();
        } catch (InterruptedException e) {
            // intentionally blank
        } finally {
            storageApp.close(true);
            micronautContext.stop();
        }
    }
}
