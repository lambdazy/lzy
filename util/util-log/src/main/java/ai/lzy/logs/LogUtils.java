package ai.lzy.logs;

import org.apache.logging.log4j.ThreadContext;

import java.util.Map;
import java.util.function.Supplier;

public final class LogUtils {
    private LogUtils() { }


    public static void withLoggingContext(Map<String, String> values, Runnable f) {
        var currentContext = ThreadContext.getImmutableContext();
        try {
            ThreadContext.putAll(values);
            f.run();
        } finally {
            ThreadContext.clearMap();
            ThreadContext.putAll(currentContext);
        }
    }


    public static <T> T withLoggingContext(Map<String, String> values, Supplier<T> f) {
        var currentContext = ThreadContext.getImmutableContext();
        try {
            ThreadContext.putAll(values);
            return f.get();
        } finally {
            ThreadContext.clearMap();
            ThreadContext.putAll(currentContext);
        }
    }
}
