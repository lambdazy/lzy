package ai.lzy.util.grpc;

import ai.lzy.logs.LogContextKey;
import io.grpc.Context;
import io.grpc.Metadata;
import lombok.Lombok;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.ThreadContext;

import java.util.HashMap;
import java.util.Map;

import static ai.lzy.util.grpc.GrpcHeaders.HEADERS;

public abstract class ContextAwareTask implements Runnable {
    private final Map<String, String> logContext;
    private final Context grpcContext;

    public ContextAwareTask() {
        logContext = ThreadContext.getContext();
        grpcContext = Context.current().fork();

        emplace(LogContextKey.REQUEST_ID, GrpcHeaders.X_REQUEST_ID);
        emplace(LogContextKey.EXECUTION_ID, GrpcHeaders.X_EXECUTION_ID);
        emplace(LogContextKey.EXECUTION_TASK_ID, GrpcHeaders.X_EXECUTION_TASK_ID);
    }

    public final void run() {
        var newHeaders = new Metadata();
        var existingHeaders = HEADERS.get(grpcContext);
        if (existingHeaders != null) {
            newHeaders.merge(existingHeaders);
        }
        prepareGrpcHeaders().forEach(newHeaders::put);

        var ctx = grpcContext.withValue(HEADERS, newHeaders);
        var previous = ctx.attach();
        try (var ignore = CloseableThreadContext.putAll(logContext).putAll(prepareLogContext())) {
            try {
                execute();
            } catch (Exception e) {
                throw Lombok.sneakyThrow(e);
            }
        } finally {
            ctx.detach(previous);
        }
    }

    protected abstract void execute();

    protected Map<String, String> prepareLogContext() {
        return new HashMap<>();
    }

    protected Map<Metadata.Key<String>, String> prepareGrpcHeaders() {
        return new HashMap<>();
    }

    protected final void emplace(String logKey, Metadata.Key<String> grpcHeader) {
        var value = GrpcHeaders.getHeader(grpcHeader);
        if (value != null) {
            logContext.putIfAbsent(logKey, value);
        }
    }
}
