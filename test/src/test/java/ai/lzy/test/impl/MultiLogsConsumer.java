package ai.lzy.test.impl;

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
