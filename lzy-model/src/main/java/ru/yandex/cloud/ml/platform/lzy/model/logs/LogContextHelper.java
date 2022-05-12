package ru.yandex.cloud.ml.platform.lzy.model.logs;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.ThreadContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class LogContextHelper {

    private final ConcurrentHashMap<String, String> logContext;

    public static final String TASK_ID = "task_id";
    public static final String USER_ID = "user_id";

    private LogContextHelper() {
        logContext = new ConcurrentHashMap<>();
    }

    public static LogContextHelper newContext() {
        return new LogContextHelper();
    }

    public auto.Runnable closeable() {
        logContext.forEach(ThreadContext::put);
        return () -> logContext.forEach((key, value) -> ThreadContext.remove(key));
    }

    public void run(Runnable f) {
        try (final var ignored = closeable()) {
            f.run();
        }
    }

    public <R> R run(Supplier<R> f) {
        try (final var ignored = closeable()) {
            return f.get();
        }
    }

    public LogContextHelper withFields(ImmutableMap<String, String> fields) {
        logContext.putAll(fields);
        return this;
    }

    public LogContextHelper withUserId(String userId) {
        logContext.put(USER_ID, userId);
        return this;
    }

    public LogContextHelper withTaskId(String taskId) {
        logContext.put(TASK_ID, taskId);
        return this;
    }


}
