package ai.lzy.test.context.dummy;

import ai.lzy.iam.test.IamContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;

import java.nio.file.Path;
import java.util.Map;

import static ai.lzy.test.context.LzyContext.DUMMY_CONTEXT_NAME;

@Singleton
@Secondary
@Requires(env = DUMMY_CONTEXT_NAME)
public class DummyIamContext implements IamContext {
    @Override
    public void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) {}

    @Override
    public void tearDown() {}
}
