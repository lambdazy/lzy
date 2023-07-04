package ai.lzy.service.debug;

import lombok.Lombok;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class InjectedFailures {
    public static final List<AtomicReference<Supplier<Throwable>>> FAIL_START_WORKFLOW_OPERATION = List.of(
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null)
    );

    public static void reset() {
        FAIL_START_WORKFLOW_OPERATION.forEach(x -> x.set(null));
    }

    public static void fail(int n) {
        failImpl(FAIL_START_WORKFLOW_OPERATION.get(n));
    }

    public static void failKafkaTopicStep() {
        fail(0);
    }

    public static void failCreateSessionStep() {
        fail(1);
    }

    public static void failCreatePortalIamSubjectStep() {
        fail(2);
    }

    public static void failAllocatePortalVmStep() {
        fail(3);
    }

    public static void failStartExecutionCompletion() {
        fail(4);
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
        public TerminateException() {}

        public TerminateException(String message) {
            super(message);
        }
    }
}
