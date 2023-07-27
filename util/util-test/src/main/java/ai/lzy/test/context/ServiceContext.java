package ai.lzy.test.context;

import java.util.Map;

public interface ServiceContext {
    void setUp(String baseConfigPath, Map<String, Object> configOverrides, String... environments) throws Exception;

    void tearDown();
}
