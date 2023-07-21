package ai.lzy.env.logs;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.FormattedMessage;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

class LogHandleImpl implements LogHandle {
    private static final String PREFIX = "[SYS] ";
    private final Logger logger;
    private final LogStreamQueue outQueue;
    private final LogStreamQueue errQueue;
    private final ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();


    public LogHandleImpl(LogStreamQueue outQueue, LogStreamQueue errQueue, Logger logger) {
        this.logger = logger;
        this.outQueue = outQueue;
        this.errQueue = errQueue;
    }

    @Override
    public void logOut(String pattern, Object... values) {
        var formatted = PREFIX + new FormattedMessage(pattern, values) + "\n";

        logger.info(formatted);
        futures.add(outQueue.add(formatted));
    }

    @Override
    public CompletableFuture<Void> logOut(InputStream stream) {
        var fut = outQueue.add(stream);
        futures.add(fut);
        return fut;
    }

    @Override
    public void logErr(String pattern, Object... values) {
        var formatted = PREFIX + new FormattedMessage(pattern, values) + "\n";

        logger.info(formatted);
        futures.add(errQueue.add(formatted));
    }

    @Override
    public CompletableFuture<Void> logErr(InputStream stream) {
        var fut = errQueue.add(stream);
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
            logger.error("Cannot close out/error streams: ", e);
        }
    }
}
