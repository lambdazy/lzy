package ai.lzy.test.context;

import java.nio.file.Path;
import java.util.Map;

public interface ServiceContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
