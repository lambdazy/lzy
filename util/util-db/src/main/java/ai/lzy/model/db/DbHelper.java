package ai.lzy.model.db;

import org.apache.logging.log4j.Logger;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

@SuppressWarnings("BusyWait")
public enum DbHelper {
    ;

    public interface Func<T> {
        T run() throws SQLException;
    }

    public interface Op {
        void run() throws SQLException;
    }

    public static <T> T withRetries(RetryPolicy retryPolicy, Logger logger, Func<T> fn)
        throws RetryCountExceededException, SQLException, InterruptedException
    {
        int delay = 0;
        for (;;) {
            if (delay > 0) {
                Thread.sleep(delay);
            }

            try {
                return fn.run();
            } catch (PSQLException e) {
                if (canRetry(e)) {
                    delay = retryPolicy.getNextDelayMs();
                    if (delay < 0) {
                        logger.error("Got retryable database error: [{}] {}. Retries limit exceeded.",
                            e.getSQLState(), e.getMessage());
                        throw new RetryCountExceededException();
                    }

                    logger.error("Got retryable database error: [{}] {}. Retry after {}ms.",
                        e.getSQLState(), e.getMessage(), delay);
                    continue;
                }

                logger.error("Got non-retryable database error: [{}] {}.", e.getSQLState(), e.getMessage());
                throw e;
            }
        }
    }

    public static <T> boolean withRetries(RetryPolicy retryPolicy, Logger logger, Func<T> fn, Consumer<T> onSuccess,
                                          IntConsumer onRetriesLimitExceeded, Consumer<Exception> onNonRetryableError)
    {
        int delay = 0;
        for (int attempt = 1; ; ++attempt) {
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ex) {
                    onNonRetryableError.accept(ex);
                    return false;
                }
            }

            try {
                var ret = fn.run();
                onSuccess.accept(ret);
                return true;
            } catch (PSQLException e) {
                if (canRetry(e)) {
                    delay = retryPolicy.getNextDelayMs();
                    if (delay >= 0) {
                        logger.error("Got retryable database error: [{}] {}. Retry after {}ms.",
                            e.getSQLState(), e.getMessage(), delay);
                        continue;
                    } else {
                        logger.error("Got retryable database error: [{}] {}. Retries limit exceeded.",
                            e.getSQLState(), e.getMessage());
                        onRetriesLimitExceeded.accept(attempt);
                    }
                } else {
                    logger.error("Got non-retryable database error: [{}] {}.", e.getSQLState(), e.getMessage());
                    onNonRetryableError.accept(e);
                }
            } catch (Exception e) {
                logger.error("Got non-retryable error: {}.", e.getMessage(), e);
                onNonRetryableError.accept(e);
            }
            return false;
        }
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
            return delay;
        }
    }

    public static RetryPolicy defaultRetryPolicy() {
        return new DefaultRetryPolicy(10, 100, 2);
    }

    public static final class RetryCountExceededException extends Exception {
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
}
