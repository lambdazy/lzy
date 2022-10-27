package ai.lzy.portal.exceptions;

public class SnapshotNotFound extends CreateSlotException {
    public SnapshotNotFound(String message) {
        super(message);
    }

    public SnapshotNotFound(Throwable cause) {
        super(cause);
    }
}
