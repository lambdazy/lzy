package ai.lzy.portal.exceptions;

public class SnapshotUniquenessException extends CreateSlotException {
    public SnapshotUniquenessException(String message) {
        super(message);
    }

    public SnapshotUniquenessException(Throwable cause) {
        super(cause);
    }
}
