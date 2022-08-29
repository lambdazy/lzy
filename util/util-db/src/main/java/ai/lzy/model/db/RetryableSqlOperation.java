package ai.lzy.model.db;

import org.postgresql.util.PSQLException;

import java.sql.SQLException;

public interface RetryableSqlOperation<T> {

    T run() throws SQLException;

    static <T> T withRetries(int retriesCount, RetryableSqlOperation<T> op) throws Exception {
        int delay = 100;
        for (int i = 0; i < retriesCount; ++i) {
            if (i > 0) {
                Thread.sleep(delay);
                delay *= 2;
            }

            try {
                return op.run();
            } catch (PSQLException e) {
                switch (e.getSQLState()) {
                    case PSQL_CannotSerializeTransaction -> {
                        System.err.printf("--> will retry after %d ms\n", delay);
                        continue;
                    }
                    // TODO: add database availability errors
                }
                throw e;
            }
        }

        throw new RetryCountExceededException();
    }

    final class RetryCountExceededException extends Exception {
    }

    String PSQL_CannotSerializeTransaction = "40001";
}
