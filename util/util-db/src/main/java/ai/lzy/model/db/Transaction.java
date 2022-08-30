package ai.lzy.model.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface Transaction {

    /**
     * @return `true` for commit, `false` for rollback
     */
    boolean execute(Connection connection) throws Exception;

    static boolean execute(Storage storage, Transaction transaction) throws DaoException {
        try (final Connection con = storage.connect()) {
            try {
                con.setAutoCommit(false); // To execute many queries in one transaction
                con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                if (transaction.execute(con)) {
                    con.commit();
                    return true;
                } else {
                    con.rollback();
                    return false;
                }
            } catch (Exception e) {
                con.rollback();
                if (e instanceof DaoException daoException) {
                    throw daoException;
                }
                throw new DaoException(e);
            } finally {
                con.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }
}
