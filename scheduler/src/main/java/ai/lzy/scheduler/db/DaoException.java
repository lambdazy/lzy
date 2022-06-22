package ai.lzy.scheduler.db;

public class DaoException extends Exception {
    public DaoException(Exception e) {
        super(e);
    }

    public DaoException(String e) {
        super(e);
    }

    public static class AcquireException extends DaoException {

        public AcquireException(Exception e) {
            super(e);
        }

        public AcquireException(String e) {
            super(e);
        }
    }

    public static class ConstraintException extends DaoException {
        public ConstraintException(Exception e) {
            super(e);
        }

        public ConstraintException(String e) {
            super(e);
        }
    }
}
