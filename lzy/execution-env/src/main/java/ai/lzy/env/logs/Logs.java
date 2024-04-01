package ai.lzy.env.logs;

import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
    private static final Logger LOG = LogManager.getLogger(Logs.class);

    private final List<LogStream> streams = new ArrayList<>();
    private final Map<String, LogStreamQueue> queues = new HashMap<>();
    protected Duration closeTimeout = Duration.ofMinutes(1);

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

        var deadline = Instant.now().plus(closeTimeout);
        for (var stream: streams) {
            LOG.debug("Awaiting stream {} to be closed", stream.name());
            stream.await(deadline);
            LOG.debug("Stream {} closed", stream.name());
        }
    }
}
