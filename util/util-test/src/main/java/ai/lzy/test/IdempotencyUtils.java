package ai.lzy.test;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public enum IdempotencyUtils {
    ;

    public record TestScenario<S extends AbstractBlockingStub<S>, T, R>(
        S stub,
        Function<S, T> preparation,
        BiFunction<S, T, R> action,
        Consumer<R> asserting
    ) {}

    public static <S extends AbstractBlockingStub<S>, T, R> void processIdempotentCallsConcurrently(
        TestScenario<S, T, R> scenario) throws InterruptedException
    {
        var stub = scenario.stub;
        var preparation = scenario.preparation;
        var action = scenario.action;
        var asserting = scenario.asserting;

        T input;
        try {
            input = preparation.apply(stub);
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("Unexpected error at preparation stage", e);
        }

        var idempotencyKey = "idempotency-key";
        S idempotentCallsClient = withIdempotencyKey(stub, idempotencyKey);

        int n = 4;
        var readyLatch = new CountDownLatch(n);
        var doneLatch = new CountDownLatch(n);
        var executor = Executors.newFixedThreadPool(n);
        var failed = new AtomicBoolean(false);

        var results = new ArrayList<R>(Collections.nCopies(n, null));

        for (int i = 0; i < n; ++i) {
            var index = i;
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    R actionResult = action.apply(idempotentCallsClient, input);
                    results.set(index, actionResult);
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        if (failed.get()) {
            throw new RuntimeException("Some of concurrent call was failed");
        }

        var firstUncommon = results.stream().filter(result -> !result.equals(results.get(0))).findFirst();
        if (firstUncommon.isPresent()) {
            throw new RuntimeException("All concurrent calls must be equal. Some call has result: "
                + results.get(0) + ", another one: " + firstUncommon.get());
        }

        asserting.accept(results.get(0));
    }

    public static <S extends AbstractBlockingStub<S>, T, R> void processIdempotentCallsSequentially(
        TestScenario<S, T, R> scenario)
    {
        var stub = scenario.stub;
        var preparation = scenario.preparation;
        var action = scenario.action;
        var asserting = scenario.asserting;

        T input;
        try {
            input = preparation.apply(stub);
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("Unexpected error at preparation stage", e);
        }

        var idempotencyKey = "idempotency-key";
        S idempotentCallsClient = withIdempotencyKey(stub, idempotencyKey);

        R firstAttempt;
        try {
            firstAttempt = action.apply(idempotentCallsClient, input);
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("Unexpected error at first attempt", e);
        }

        R secondAttempt;
        try {
            secondAttempt = action.apply(idempotentCallsClient, input);
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("Unexpected error at second attempt", e);
        }

        if (!firstAttempt.equals(secondAttempt)) {
            throw new RuntimeException("Second attempt result not equal to the first one: first: "
                + firstAttempt + " second: " + secondAttempt);
        }

        asserting.accept(firstAttempt);
        asserting.accept(secondAttempt);
    }
}
