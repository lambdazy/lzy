package ai.lzy.util.grpc;

import com.google.common.base.Stopwatch;
import com.google.common.primitives.Longs;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class GrpcChannels {

    public static void awaitTermination(ManagedChannel channel, Class<?> caller) {
        channel.shutdown();
        try {
            while (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
                LogManager.getLogger(caller).debug("Shutting down {} ...", caller.getSimpleName());
            }
        } catch (InterruptedException e) {
            LogManager.getLogger(caller).warn("{} shutdown was interrupted", caller.getSimpleName());
            Thread.currentThread().interrupt();
        }
    }

    public static void awaitTermination(ManagedChannel channel, Duration timeout, Class<?> caller) {
        channel.shutdown();
        try {
            new Awaiter(timeout, Duration.ofMillis(15), 2).await(channel::isTerminated, result -> result);
        } catch (IllegalStateException e) {
            LogManager.getLogger(caller).warn("{} was not shut down in {}", caller.getSimpleName(), timeout);
        } catch (UncheckedInterruptedException e) {
            LogManager.getLogger(caller).warn("{} shutdown was interrupted", caller.getSimpleName());
        }
    }

    private GrpcChannels() {
    }

    public static final class Awaiter {
        private final Duration timeout;
        private final Duration initialInterval;
        private final double multiplier;

        public Awaiter(Duration timeout, Duration initialInterval, double multiplier) {
            this.timeout = timeout;
            this.initialInterval = initialInterval;
            this.multiplier = multiplier;
        }

        /**
         * Gets value from supplier while done returns false with increasing interval.
         * Returns the value when done returns true.
         *
         * @param supplier value supplier
         * @param done     criterion for await to complete
         * @param <T>      value type
         * @return the last value
         * @throws IllegalStateException done returned false after timeout expired
         */
        public <T> T await(Supplier<T> supplier, Predicate<T> done) {
            Stopwatch sw = Stopwatch.createStarted();

            long interval = initialInterval.toMillis();

            T t = supplier.get();
            while (!done.test(t)) {
                Duration elapsed = sw.elapsed();
                if (elapsed.compareTo(timeout) > 0) {
                    throw new IllegalStateException("Await did not complete in time: " + t);
                }

                try {
                    Thread.sleep(Longs.constrainToRange(timeout.toMillis() - elapsed.toMillis(), 0, interval));
                } catch (InterruptedException e) {
                    throw new UncheckedInterruptedException(e);
                }

                interval = Math.max(Math.round(interval * multiplier), interval + 1);
                t = supplier.get();
            }
            return t;
        }
    }

    public static class UncheckedInterruptedException extends RuntimeException {
        public UncheckedInterruptedException(InterruptedException e) {
            super(e);
        }
    }
}
