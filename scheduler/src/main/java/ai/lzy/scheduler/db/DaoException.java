package ai.lzy.scheduler.db;

public class DaoException extends Exception {
    public DaoException(Exception e) {
        super(e);
    }

    public DaoException(String e) {
        super(e);
    }

    public static class AcquireException extends Exception {

        public AcquireException(Exception e) {
            super(e);
        }

        public AcquireException(String e) {
            super(e);
        }
    }
}
