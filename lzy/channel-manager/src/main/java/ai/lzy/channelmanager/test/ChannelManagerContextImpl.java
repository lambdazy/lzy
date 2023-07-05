package ai.lzy.channelmanager.test;

import ai.lzy.channelmanager.ChannelManagerApp;
import ai.lzy.test.context.ChannelManagerContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ai.lzy.channelmanager.test.ChannelManagerContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class ChannelManagerContextImpl implements ChannelManagerContext {
    public static final String ENV_NAME = "common_channel_manager_test";

    private ApplicationContext micronautContext;
    private ChannelManagerApp channelManagerApp;

    @Override
    public void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws IOException {
        try (var file = new FileInputStream(config.toFile())) {
            var actualConfig = new YamlPropertySourceLoader().read("channel-manager", file);
            actualConfig.putAll(runtimeConfig);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        channelManagerApp = micronautContext.getBean(ChannelManagerApp.class);
        channelManagerApp.start();
    }

    @Override
    public void tearDown() {
        channelManagerApp.shutdown();
        try {
            channelManagerApp.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // intentionally blank
        } finally {
            channelManagerApp.shutdownNow();
            micronautContext.stop();
        }
    }
}
