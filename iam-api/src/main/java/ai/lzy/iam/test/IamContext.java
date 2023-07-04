package ai.lzy.iam.test;

import java.nio.file.Path;
import java.util.Map;

public interface IamContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
