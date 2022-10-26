package ai.lzy.util.grpc;

import io.grpc.Context;
import lombok.Lombok;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.ThreadContext;

import java.util.Map;

public abstract class ContextAwareTask implements Runnable {
    private final Map<String, String> logContext;
    private final Context grpcContext;

    public ContextAwareTask() {
        logContext = ThreadContext.getContext();
        grpcContext = Context.current().fork();
    }

    public final void run() {
        var previous = grpcContext.attach();
        try (var ignore = CloseableThreadContext.putAll(logContext)) {
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
}
