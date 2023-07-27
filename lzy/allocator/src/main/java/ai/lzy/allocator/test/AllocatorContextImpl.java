package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.test.context.AllocatorContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

import static ai.lzy.allocator.test.AllocatorContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class AllocatorContextImpl implements AllocatorContext {
    public static final String ENV_NAME = "common_allocator_test";

    private ApplicationContext micronautContext;
    private AllocatorMain allocatorApp;

    @Override
    public void setUp(String baseConfigPath, Map<String, Object> configOverrides, String... environments)
        throws IOException
    {
        try (var file = new FileInputStream(Objects.requireNonNull(baseConfigPath))) {
            var actualConfig = new YamlPropertySourceLoader().read("allocator", file);
            actualConfig.putAll(configOverrides);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        allocatorApp = micronautContext.getBean(AllocatorMain.class);
        allocatorApp.start();
    }

    @Override
    public void tearDown() {
        try {
            allocatorApp.destroyAllForTests();
        } catch (SQLException e) {
            // intentionally blank
        } finally {
            allocatorApp.stop(false);

            try {
                allocatorApp.awaitTermination();
            } catch (InterruptedException e) {
                // intentionally blank
            } finally {
                allocatorApp.stop(true);
                micronautContext.stop();
            }
        }
    }

    public AllocatorServiceDecorator allocator() {
        return micronautContext.getBean(AllocatorServiceDecorator.class);
    }

    public ApplicationContext getMicronautContext() {
        return micronautContext;
    }
}
