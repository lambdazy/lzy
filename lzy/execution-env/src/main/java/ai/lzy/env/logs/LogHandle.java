package ai.lzy.env.logs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface LogHandle extends AutoCloseable {
    Logger LOG = LogManager.getLogger(LogHandle.class);

    void logSysOut(String message);

    CompletableFuture<Void> logOut(InputStream stream, boolean system);

    void logSysErr(String message);

    CompletableFuture<Void> logErr(InputStream stream, boolean system);

    void close();

    @VisibleForTesting
    static LogHandle emptyForTests() {
        return builder()
            .build("out", "err");
    }

    static Builder builder() {
        return new Builder();
    }

    class Builder {
        private final ArrayList<LogWriter> writers = new ArrayList<>();
        private Function<String, String> systemFormatter = LogHandleImpl::formatSystemLog;

        public Builder() {
        }

        public Builder withWriters(LogWriter... writersArr) {
            writers.addAll(Arrays.asList(writersArr));
            return this;
        }

        public Builder withSystemLogFormatter(Function<String, String> formatter) {
            systemFormatter = formatter;
            return this;
        }

        public LogHandle build(String outName, String errName) {
            var outQueue = new LogStreamQueue(outName, writers);
            outQueue.start();

            var errQueue = new LogStreamQueue(errName, writers);
            errQueue.start();

            return new LogHandleImpl(outQueue, errQueue, systemFormatter);
        }
    }
}
