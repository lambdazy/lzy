package ai.lzy.test.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

public class MultiLogsConsumer extends BaseConsumer<MultiLogsConsumer> {
    private final List<BaseConsumer<?>> consumers = new ArrayList<>();

    public MultiLogsConsumer(BaseConsumer<?>... consumers) {
        this.consumers.addAll(Arrays.asList(consumers));
    }

    @Override
    public void accept(OutputFrame outputFrame) {
        consumers.forEach(baseConsumer -> baseConsumer.accept(outputFrame));
    }
}
