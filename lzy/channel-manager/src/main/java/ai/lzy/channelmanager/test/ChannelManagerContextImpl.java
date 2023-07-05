package ai.lzy.channelmanager.test;

import ai.lzy.channelmanager.ChannelManagerMain;
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

import static ai.lzy.channelmanager.test.ChannelManagerContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class ChannelManagerContextImpl implements ChannelManagerContext {
    public static final String ENV_NAME = "common_channel_manager_test";

    private ApplicationContext micronautContext;
    private ChannelManagerMain channelManagerApp;

    @Override
    public void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws IOException {
        try (var file = new FileInputStream(config.toFile())) {
            var actualConfig = new YamlPropertySourceLoader().read("channel-manager", file);
            actualConfig.putAll(runtimeConfig);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        channelManagerApp = micronautContext.getBean(ChannelManagerMain.class);
        channelManagerApp.start();
    }

    @Override
    public void tearDown() {
        try {
            channelManagerApp.close();
        } catch (InterruptedException e) {
            // intentionally blank
        } finally {
            micronautContext.stop();
        }
    }
}
