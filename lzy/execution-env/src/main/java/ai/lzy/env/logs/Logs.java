package ai.lzy.env.logs;

import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Class to manage LogStream instances
 * To create own LogStreams, user must extend this class and call stream functions
 */
public class Logs implements AutoCloseable {
    private final List<LogStream> streams = new ArrayList<>();
    private final Map<String, LogStreamQueue> queues = new HashMap<>();

    /**
     * Init all log streams
     * Must be called before writing to stream
     */
    public void init(List<LogWriter> writers) {
        for (var stream: streams) {
            // Using one queue for streams with same names
            var queue = queues.computeIfAbsent(stream.name(), k -> {
                var q = new LogStreamQueue(stream.name(), writers);
                q.start();
                return q;
            });
            stream.init(queue);
            streams.add(stream);
        }
    }

    protected LogStream stream(String name) {
        return stream(name, null);
    }

    protected LogStream stream(String name, @Nullable Function<String, String> formatter) {
        var stream =  new LogStream(name, formatter);
        streams.add(stream);
        return stream;
    }

    @Override
    public void close() {
        for (var queue: queues.values()) {
            try {
                queue.close();
            } catch (InterruptedException e) {
                // ignored
            }
        }

        for (var stream: streams) {
            stream.await();
        }
    }
}
