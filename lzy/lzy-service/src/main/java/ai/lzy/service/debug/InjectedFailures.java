package ai.lzy.service.debug;

import lombok.Lombok;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class InjectedFailures {
    public static final List<AtomicReference<Supplier<Throwable>>> FAIL_LZY_SERVICE = List.of(
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null)
    );

    public static void reset() {
        FAIL_LZY_SERVICE.forEach(x -> x.set(null));
    }

    public static void fail(int n) {
        failImpl(FAIL_LZY_SERVICE.get(n));
    }

    public static void fail0() {
        fail(0);
    }

    public static void fail1() {
        fail(1);
    }

    public static void fail2() {
        fail(2);
    }

    public static void fail3() {
        fail(3);
    }

    public static void fail4() {
        fail(4);
    }

    public static void fail5() {
        fail(5);
    }

    public static void fail6() {
        fail(6);
    }

    public static void fail7() {
        fail(7);
    }

    public static void fail8() {
        fail(8);
    }

    public static void fail9() {
        fail(9);
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
