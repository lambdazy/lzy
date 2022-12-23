package ai.lzy.service.graph.debug;

import lombok.Lombok;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class InjectedFailures {
    public static final List<AtomicReference<Supplier<Throwable>>> FAIL_EXECUTE_GRAPH = List.of(
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null)
    );

    public static void reset() {
        FAIL_EXECUTE_GRAPH.forEach(x -> x.set(null));
    }

    public static void failExecuteGraph(int n) {
        failImpl(FAIL_EXECUTE_GRAPH.get(n));
    }

    public static void failExecuteGraph0() {
        failExecuteGraph(0);
    }

    public static void failExecuteGraph1() {
        failExecuteGraph(1);
    }

    public static void failExecuteGraph2() {
        failExecuteGraph(2);
    }

    public static void failExecuteGraph3() {
        failExecuteGraph(3);
    }

    public static void failExecuteGraph4() {
        failExecuteGraph(4);
    }

    public static void failExecuteGraph5() {
        failExecuteGraph(5);
    }

    public static void failExecuteGraph6() {
        failExecuteGraph(6);
    }

    public static void failExecuteGraph7() {
        failExecuteGraph(7);
    }

    public static void failExecuteGraph8() {
        failExecuteGraph(8);
    }

    private static void failImpl(AtomicReference<Supplier<Throwable>> ref) {
        final var fn = ref.get();
        if (fn == null) {
            return;
        }
        final var th = fn.get();
        if (th != null) {
            ref.set(null);
            throw Lombok.sneakyThrow(th);
        }
    }

    public static final class TerminateException extends Error {
        public TerminateException() {
        }

        public TerminateException(String message) {
            super(message);
        }
    }
}
