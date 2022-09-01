package ai.lzy.kharon.workflow;


import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.RetryerBuilder;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

@SuppressWarnings("UnstableApiUsage")
public class WorkflowUtils {
    private WorkflowUtils() {
    }

    /**
     * Restarts `action` until deadline.
     *
     * @throws ExecutionException -- if unexpected exception was thrown while action
     * @throws RetryException     -- if there was no one successful attempts before deadline
     */
    public static void executeWithRetry(BiConsumer<Long, Throwable> exceptionsHandler,
                                        Collection<Class<? extends Throwable>> expectedExceptions,
                                        ThrowingRunnable action,
                                        Instant deadline)
        throws ExecutionException, RetryException {

        var retry = RetryerBuilder.<Void>newBuilder()
            .withRetryListener(new RetryListener() {
                @Override
                public <V> void onRetry(Attempt<V> attempt) {
                    if (attempt.hasException()) {
                        exceptionsHandler.accept(attempt.getAttemptNumber(), attempt.getExceptionCause());
                    }
                }
            }).withStopStrategy(attempt -> Instant.now().isAfter(deadline));

        expectedExceptions.forEach(retry::retryIfExceptionOfType);

        retry.build().call(() -> {
            action.run();
            return null;
        });
    }

    /**
     * Restarts `action` until deadline.
     *
     * @throws ExecutionException -- if unexpected exception was thrown while action
     * @throws RetryException     -- if there was no one successful attempts before deadline
     */
    public static <T> T executeWithRetry(BiConsumer<Long, Throwable> exceptionsHandler,
                                         Collection<Class<? extends Throwable>> expectedExceptions,
                                         Callable<T> action, Instant deadline)
        throws ExecutionException, RetryException {

        var retry = RetryerBuilder.<T>newBuilder()
            .withRetryListener(new RetryListener() {
                @Override
                public <V> void onRetry(Attempt<V> attempt) {
                    if (attempt.hasException()) {
                        exceptionsHandler.accept(attempt.getAttemptNumber(), attempt.getExceptionCause());
                    }
                }
            }).withStopStrategy(attempt -> Instant.now().isAfter(deadline));

        expectedExceptions.forEach(retry::retryIfExceptionOfType);

        return retry.build().call(action);
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
