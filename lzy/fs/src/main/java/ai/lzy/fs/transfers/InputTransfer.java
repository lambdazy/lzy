package ai.lzy.fs.transfers;

import java.io.IOException;
import java.io.OutputStream;

public interface InputTransfer {

    // Return number of bytes read, or -1 if EOF
    // Throws ReadException if non-retryable error occurs while reading
    // Throws IOException if an error occurs while writing to sink
    int readInto(OutputStream sink) throws ReadException, IOException;

    class ReadException extends Exception {
        public ReadException(String message, Exception cause) {
            super(message, cause);
        }
    }

    default void close() {}
}
