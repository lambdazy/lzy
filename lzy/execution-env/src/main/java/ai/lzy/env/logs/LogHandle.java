package ai.lzy.env.logs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public interface LogHandle extends AutoCloseable {
    Logger LOG = LogManager.getLogger(LogHandle.class);

    void logOut(String pattern, Object... values);

    CompletableFuture<Void> logOut(InputStream stream);

    void logErr(String pattern, Object... values);

    CompletableFuture<Void> logErr(InputStream stream);

    void close();

    @VisibleForTesting
    static LogHandle empty() {
        return builder()
            .build();
    }

    static Builder builder() {
        return new Builder(LOG);
    }

    class Builder {
        private final ArrayList<LogWriter> writers = new ArrayList<>();
        private final Logger logger;

        public Builder(Logger logger) {
            this.logger = logger;
        }

        public Builder withWriters(LogWriter... writersArr) {
            writers.addAll(Arrays.asList(writersArr));
            return this;
        }

        public LogHandle build() {
            var outQueue = new LogStreamQueue("out", writers, logger);
            outQueue.start();

            var errQueue = new LogStreamQueue("err", writers, logger);
            errQueue.start();

            return new LogHandleImpl(outQueue, errQueue, logger);
        }
    }
}
