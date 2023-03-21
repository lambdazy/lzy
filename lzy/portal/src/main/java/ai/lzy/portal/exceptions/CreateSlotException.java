package ai.lzy.portal.exceptions;

public class CreateSlotException extends Exception {
    public CreateSlotException(String message) {
        super(message);
    }

    public CreateSlotException(Throwable cause) {
        super(cause);
    }

    public CreateSlotException(String message, Throwable cause) {
        super(message, cause);
    }
}
