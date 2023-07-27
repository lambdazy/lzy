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
import java.util.Map;
import java.util.Objects;

import static ai.lzy.channelmanager.test.ChannelManagerContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class ChannelManagerContextImpl implements ChannelManagerContext {
    public static final String ENV_NAME = "common_channel_manager_test";

    private ApplicationContext micronautContext;
    private ChannelManagerMain channelManagerApp;

    @Override
    public void setUp(String baseConfigPath, Map<String, Object> configOverrides, String... environments)
        throws IOException
    {
        try (var file = new FileInputStream(Objects.requireNonNull(baseConfigPath))) {
            var actualConfig = new YamlPropertySourceLoader().read("channel-manager", file);
            actualConfig.putAll(configOverrides);
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

    public ApplicationContext getMicronautContext() {
        return micronautContext;
    }
}
