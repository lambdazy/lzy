package ai.lzy.storage;

public class StorageException extends Exception {

    public enum Status {
        ALREADY_EXISTS,
        INTERNAL_ERROR
    }

    private final Status status;

    public static StorageException alreadyExists(String message) {
        return new StorageException(Status.ALREADY_EXISTS, message);
    }

    public static StorageException internalError(String message, Exception reason) {
        return new StorageException(Status.INTERNAL_ERROR, message, reason);
    }

    public StorageException(Status status) {
        super();
        this.status = status;
    }

    public StorageException(Status status, String message) {
        super(message);
        this.status = status;
    }

    public StorageException(Status status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }
}
