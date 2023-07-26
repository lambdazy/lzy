package ai.lzy.test.context.dummy;

import ai.lzy.test.context.*;
import io.micronaut.context.annotation.Secondary;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
@Secondary
public class DummyServiceContext implements IamContext, AllocatorContext, ChannelManagerContext, GraphExecutorContext,
    SchedulerContext, LzyServiceContext, StorageServiceContext, WhiteboardServiceContext
{
    @Override
    public void setUp(@Nullable String baseConfigPath, Map<String, Object> configOverrides, String... environments) {}

    @Override
    public void tearDown() {}
}
