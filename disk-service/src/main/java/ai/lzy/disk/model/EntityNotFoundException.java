package ai.lzy.disk.model;

public class EntityNotFoundException extends Exception {

    public EntityNotFoundException(String message) {
        super(message);
    }

    public EntityNotFoundException(Exception e) {
        super(e);
    }
}
