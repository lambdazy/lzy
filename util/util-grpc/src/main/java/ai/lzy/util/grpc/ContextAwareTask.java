package ai.lzy.util.grpc;

import ai.lzy.logs.LogContextKey;
import io.grpc.Context;
import io.grpc.Metadata;
import lombok.Lombok;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.ThreadContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class ContextAwareTask implements Runnable {
    private final Map<String, String> logContext;
    private final Context grpcContext;

    public ContextAwareTask() {
        logContext = ThreadContext.getContext();
        grpcContext = Context.current().fork();

        emplace(LogContextKey.REQUEST_ID, GrpcHeaders.X_REQUEST_ID);
        emplace(LogContextKey.EXECUTION_ID, GrpcHeaders.X_EXECUTION_ID);
    }

    public final void run() {
        var previous = grpcContext.attach();
        try (var ignore = CloseableThreadContext.putAll(logContext).putAll(prepareLogContext())) {
            try {
                execute();
            } catch (Exception e) {
                throw Lombok.sneakyThrow(e);
            }
        } finally {
            grpcContext.detach(previous);
        }
    }

    protected abstract void execute();

    protected Map<String, String> prepareLogContext() {
        return new HashMap<>();
    }

    protected final void emplace(String logKey, Metadata.Key<String> grpcHeader) {
        logContext.computeIfAbsent(logKey, x -> fromHeader(grpcHeader));
    }

    protected static String fromHeader(Metadata.Key<String> header) {
        return Optional.ofNullable(GrpcHeaders.getHeader(header)).orElse("unknown");
    }
}
