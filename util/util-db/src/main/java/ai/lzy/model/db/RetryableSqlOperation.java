package ai.lzy.model.db;

import org.apache.logging.log4j.Logger;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;

public interface RetryableSqlOperation<T> {

    T run(int attemptNo) throws SQLException;

    @SuppressWarnings("BusyWait")
    static <T> T withRetries(RetryPolicy retryPolicy, Logger logger, RetryableSqlOperation<T> op)
        throws RetryCountExceededException, SQLException, InterruptedException
    {
        for (int attempt = 1; ; ++attempt) {
            try {
                return op.run(attempt);
            } catch (PSQLException e) {
                if (canRetry(e)) {
                    var delay = retryPolicy.getNextDelayMs();
                    if (delay < 0) {
                        logger.error("Got retryable database error: [{}] {}. Retries limit exceeded.",
                            e.getSQLState(), e.getMessage());
                        throw new RetryCountExceededException();
                    }

                    logger.error("Got retryable database error: [{}] {}. Retry after {}ms.",
                        e.getSQLState(), e.getMessage(), delay);
                    Thread.sleep(delay);
                    continue;
                }

                logger.error("Got non-retryable database error: [{}] {}.", e.getSQLState(), e.getMessage());
                throw e;
            }
        }
    }

    interface RetryPolicy {
        int getNextDelayMs();
    }

    class DefaultRetryPolicy implements RetryPolicy {
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

    static RetryPolicy defaultRetryPolicy() {
        return new DefaultRetryPolicy(10, 100, 2);
    }

    final class RetryCountExceededException extends Exception {
    }

    String PSQL_CannotSerializeTransaction = "40001";

    static boolean canRetry(PSQLException e) {
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
