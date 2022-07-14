package ai.lzy.scheduler.db;

import javax.persistence.criteria.CriteriaBuilder;
import java.sql.Connection;
import java.sql.SQLException;

public class Utils {
    public interface Transaction {
        void execute(Connection connection) throws Exception;
    }

    public static void executeInTransaction(Storage storage, Transaction transaction)
            throws DaoException {
        try (final Connection con = storage.connect()) {
            try {
                con.setAutoCommit(false); // To execute many queries in one transaction
                transaction.execute(con);
                con.commit();
            } catch (Exception e) {
                if (e.getCause() instanceof InterruptedException) {
                    throw new DaoException(new InterruptedException());
                }
                if (e instanceof DaoException) {
                    throw (DaoException) e;
                }
                con.rollback();
                throw new DaoException(e);
            } finally {
                con.setAutoCommit(true);
            }
        } catch (SQLException e) {
            if (e.getCause() instanceof InterruptedException) {
                throw new DaoException(new InterruptedException());
            }
            throw new DaoException(e);
        }
    }
}
