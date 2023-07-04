package ai.lzy.storage.test;

import ai.lzy.storage.App;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static ai.lzy.storage.test.StorageContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class StorageContextImpl implements StorageContext {
    public static final String ENV_NAME = "common_storage_context";

    private ApplicationContext micronautContext;
    private App storageApp;

    @Override
    public void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws IOException {
        try (var file = new FileInputStream(config.toFile())) {
            var actualConfig = new YamlPropertySourceLoader().read("storage", file);
            actualConfig.putAll(runtimeConfig);
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
