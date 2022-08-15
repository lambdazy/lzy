package ai.lzy.model.db;

public class NotFoundException extends DaoException {
    public NotFoundException(Exception e) {
        super(e);
    }

    public NotFoundException(String e) {
        super(e);
    }
}
