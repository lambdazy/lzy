package ai.lzy.logs;

import org.apache.logging.log4j.CloseableThreadContext;

import java.util.Map;
import java.util.function.Supplier;

public final class LogUtils {
    private LogUtils() { }

    public static void withLoggingContext(Map<String, String> additionalContext, Runnable f) {
        try (var ignore = CloseableThreadContext.putAll(additionalContext)) {
            f.run();
        }
    }

    public static <T> T withLoggingContext(Map<String, String> additionalContext, Supplier<T> f) {
        try (var ignore = CloseableThreadContext.putAll(additionalContext)) {
            return f.get();
        }
    }
}
