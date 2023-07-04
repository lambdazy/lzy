package ai.lzy.channelmanager.test;

import java.nio.file.Path;
import java.util.Map;

public interface ChannelManagerContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
