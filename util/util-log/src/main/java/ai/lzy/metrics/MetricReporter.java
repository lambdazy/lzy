package ai.lzy.metrics;

import java.io.Closeable;

public interface MetricReporter extends Closeable {
    void start();

    void stop();

    default void close() {
        this.stop();
    }
}
