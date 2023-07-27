package ai.lzy.whiteboard.test;

import ai.lzy.test.context.WhiteboardServiceContext;
import ai.lzy.whiteboard.WhiteboardApp;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static ai.lzy.whiteboard.test.WhiteboardServiceContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class WhiteboardServiceContextImpl implements WhiteboardServiceContext {
    public static final String ENV_NAME = "common_whiteboard_context";

    private ApplicationContext micronautContext;
    private WhiteboardApp whiteboardApp;

    @Override
    public void setUp(String baseConfigPath, Map<String, Object> configOverrides, String... environments)
        throws IOException
    {
        try (var file = new FileInputStream(Objects.requireNonNull(baseConfigPath))) {
            var actualConfig = new YamlPropertySourceLoader().read("whiteboard", file);
            actualConfig.putAll(configOverrides);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        whiteboardApp = micronautContext.getBean(WhiteboardApp.class);
        whiteboardApp.start();
    }

    @Override
    public void tearDown() {
        whiteboardApp.stop();
        try {
            whiteboardApp.awaitTermination();
        } catch (InterruptedException e) {
            // intentionally blank
        } finally {
            micronautContext.close();
        }
    }
}
