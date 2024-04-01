package ai.lzy.env.logs;

import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class LogStream {
    public static final Logger LOG = LogManager.getLogger(LogStream.class);

    @Nullable
    private volatile LogStreamQueue queue;

    private final String streamName;
    private final ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
    private final Function<String, String> formatter;

    LogStream(String streamName, @Nullable Function<String, String> formatter) {
        this.streamName = streamName;
        this.formatter = formatter != null ? formatter : s -> s;
    }

    void init(LogStreamQueue queue) {
        this.queue = queue;
    }

    void await(Instant deadline) {
        for (var fut : futures) {
            try {
                fut.get(Instant.now().until(deadline, ChronoUnit.MILLIS), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Cannot await stream {}: ", streamName, e);
            } catch (TimeoutException e) {
                LOG.error("Timeout while awaiting logs stream {} completion", streamName);
                return;
            }
        }
    }

    public void log(String message) {
        assert queue != null: "Stream %s not initialized".formatted(streamName);

        var formatted = formatter.apply(message);
        futures.add(queue.add(formatted));
    }

    public CompletableFuture<Void> log(InputStream stream) {
        assert queue != null: "Stream %s not initialized".formatted(streamName);

        var fut = queue.add(stream, formatter);
        futures.add(fut);
        return fut;
    }

    public String name() {
        return streamName;
    }
}
