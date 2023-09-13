package ai.lzy.env.logs;

import jakarta.annotation.Nullable;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

class LogHandleImpl implements LogHandle {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String LOG_PATTERN = "[SYS] %s - %s";

    private final LogStreamQueue outQueue;
    private final LogStreamQueue errQueue;
    private final ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
    private final Function<String, String> systemFormatter;

    public LogHandleImpl(LogStreamQueue outQueue, LogStreamQueue errQueue,
                         @Nullable Function<String, String> systemFormatter)
    {
        this.outQueue = outQueue;
        this.errQueue = errQueue;
        this.systemFormatter = systemFormatter != null ? systemFormatter : s -> s;
    }

    @Override
    public void logSysOut(String message) {
        var formatted = systemFormatter.apply(message);
        futures.add(outQueue.add(formatted));
    }

    @Override
    public CompletableFuture<Void> logOut(InputStream stream, boolean system) {
        var fut = outQueue.add(stream, system ? systemFormatter : null);
        futures.add(fut);
        return fut;
    }

    @Override
    public void logSysErr(String message) {
        var formatted = systemFormatter.apply(message);
        futures.add(errQueue.add(formatted));
    }

    @Override
    public CompletableFuture<Void> logErr(InputStream stream, boolean system) {
        var fut = errQueue.add(stream, system ? systemFormatter : null);
        futures.add(fut);
        return fut;
    }

    @Override
    public void close() {
        try {
            for (var fut : futures) {
                fut.get();
            }
            outQueue.close();
            errQueue.close();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Cannot close out/error streams: ", e);
        }
    }

    public static String formatSystemLog(String message) {
        return LOG_PATTERN.formatted(
            DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneOffset.UTC)),
            message);
    }
}
