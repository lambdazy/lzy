package ai.lzy.model.db;

import org.apache.logging.log4j.Logger;

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
            } catch (SQLException e) {
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

    static boolean canRetry(SQLException e) {
        switch (e.getSQLState()) {
            case PSQL_CannotSerializeTransaction -> {
                return true;
            }
        }
        return false;
    }
}
