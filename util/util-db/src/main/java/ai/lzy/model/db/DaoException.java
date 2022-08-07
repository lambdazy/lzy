package ai.lzy.model.db;

public class DaoException extends Exception {
    public DaoException(Exception e) {
        super(e);
    }

    public DaoException(String e) {
        super(e);
    }
}
