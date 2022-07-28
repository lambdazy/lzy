package ai.lzy.model.exceptions;

public class EntityNotFoundException extends Exception {

    public EntityNotFoundException(String message) {
        super(message);
    }

    public EntityNotFoundException(Exception e) {
        super(e);
    }
}
