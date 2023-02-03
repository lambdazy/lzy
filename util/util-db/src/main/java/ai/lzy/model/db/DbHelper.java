package ai.lzy.model.db;

import org.apache.logging.log4j.Logger;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;

@SuppressWarnings({"BusyWait"})
public enum DbHelper {
    ;

    public interface Func<T> {
        T run() throws SQLException;
    }

    public interface FuncV {
        void run() throws SQLException;
    }

    public interface ErrorAdapter<E extends Exception> {
        E fail(Exception e);
    }

    public static <T> T withRetries(Logger logger, Func<T> fn) throws Exception {
        return withRetries(defaultRetryPolicy(), logger, fn, ex -> ex);
    }

    public static void withRetries(Logger logger, FuncV fn) throws Exception {
        withRetries(defaultRetryPolicy(), logger, fn, ex -> ex);
    }

    public static <T> T withRetries(RetryPolicy retryPolicy, Logger logger, Func<T> fn) throws Exception {
        return withRetries(retryPolicy, logger, fn, ex -> ex);
    }

    public static void withRetries(RetryPolicy retryPolicy, Logger logger, FuncV fn) throws Exception {
        withRetries(retryPolicy, logger, fn, ex -> ex);
    }

    public static <T, E extends Exception> T withRetries(RetryPolicy retryPolicy, Logger logger, Func<T> fn,
                                                         ErrorAdapter<E> error) throws E
    {
        int delay = 0;
        for (int attempt = 1; ; ++attempt) {
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    throw error.fail(e);
                }
            }

            try {
                return fn.run();
            } catch (PSQLException e) {
                if (canRetry(e)) {
                    delay = retryPolicy.getNextDelayMs();
                    if (delay >= 0) {
                        logger.error("Got retryable database error #{}: [{}] {}. Retry after {}ms.",
                            attempt, e.getSQLState(), e.getMessage(), delay);
                        //noinspection UnnecessaryContinue
                        continue;
                    } else {
                        logger.error("Got retryable database error: [{}] {}. Retries limit {} exceeded.",
                            e.getSQLState(), e.getMessage(), attempt);
                        throw error.fail(new RetryCountExceededException(attempt));
                    }
                } else {
                    logger.error("Got non-retryable database error: [{}] {}.", e.getSQLState(), e.getMessage());
                    throw error.fail(e);
                }
            } catch (Exception e) {
                logger.error("Got non-retryable error: {}.", e.getMessage(), e);
                throw error.fail(e);
            }
        }
    }

    public static <E extends Exception> void withRetries(RetryPolicy retryPolicy, Logger logger, FuncV fn,
                                                         ErrorAdapter<E> error) throws E
    {
        withRetries(
            retryPolicy,
            logger,
            () -> {
                fn.run();
                return (Void) null;
            },
            error);
    }

    interface RetryPolicy {
        int getNextDelayMs();
    }

    public static final class DefaultRetryPolicy implements RetryPolicy {
        private int attempts;
        private int nextDelay;
        private final int coeff;

        public DefaultRetryPolicy(int attempts, int nextDelay, int coeff) {
            this.attempts = attempts;
            this.nextDelay = nextDelay;
            this.coeff = coeff;
        }

        @Override
        public int getNextDelayMs() {
            if (--attempts < 0) {
                return -1;
            }
            var delay = nextDelay;
            nextDelay *= coeff;
            return delay + (int) ((delay / 5) * (Math.random() - 0.5));
        }
    }

    public static RetryPolicy defaultRetryPolicy() {
        return new DefaultRetryPolicy(10, 50, 2);
    }

    public static final class RetryCountExceededException extends Exception {
        public RetryCountExceededException(int count) {
            super("Database retries limit " + count + " exceeded.");
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final String PSQL_CannotSerializeTransaction = "40001";

    private static boolean canRetry(PSQLException e) {
        if (e.getSQLState() == null) {
            if (e.getCause() instanceof PSQLException ex) {
                e = ex;
            } else if (e.getCause() != null && e.getCause().getCause() instanceof PSQLException ex) {
                e = ex;
            } else {
                return false;
            }
        }

        if (PSQL_CannotSerializeTransaction.equals(e.getSQLState())) {
            return true;
        }

        return PSQLState.isConnectionError(e.getSQLState());
    }

    public static boolean isUniqueViolation(Exception e, String constraint) {
        if (e instanceof PSQLException ex) {
            if (!PSQLState.UNIQUE_VIOLATION.getState().equals(ex.getSQLState())) {
                return false;
            }

            var serverError = ex.getServerErrorMessage();
            if (serverError == null) {
                return false;
            }

            return constraint.equals(serverError.getConstraint());
        }

        return false;
    }
}
