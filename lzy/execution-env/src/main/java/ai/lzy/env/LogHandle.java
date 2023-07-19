package ai.lzy.env;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public interface LogHandle extends AutoCloseable {
    void logOut(String pattern, Object... values);

    CompletableFuture<Void> logOut(InputStream stream);

    void logErr(String pattern, Object... values);

    CompletableFuture<Void> logErr(InputStream stream);

    void close();

    @VisibleForTesting
    static LogHandle empty() {
        return new LogHandle() {
            private static final Logger LOG = LogManager.getLogger(LogHandle.class);

            @Override
            public void close() {
            }

            @Override
            public void logOut(String pattern, Object... values) {
            }

            @Override
            public CompletableFuture<Void> logOut(InputStream stream) {
                String res = null;
                try {
                    res = IOUtils.toString(stream, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                LOG.info(res);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void logErr(String pattern, Object... values) {
            }

            @Override
            public CompletableFuture<Void> logErr(InputStream stream) {
                String res = null;
                try {
                    res = IOUtils.toString(stream, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                LOG.error(res);
                return CompletableFuture.completedFuture(null);
            }
        };
    }
}
