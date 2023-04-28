package ai.lzy.util.grpc;

import io.grpc.Context;
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
        if (!logContext.containsKey("rid")) {
            var reqid = Optional.ofNullable(GrpcHeaders.getRequestId()).orElse("unknown");
            logContext.put("rid", reqid);
        }
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
}
