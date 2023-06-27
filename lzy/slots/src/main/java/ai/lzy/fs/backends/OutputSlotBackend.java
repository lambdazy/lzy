package ai.lzy.fs.backends;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;

public interface OutputSlotBackend {
    /**
     * Blocking operation to prepare output slot data for transfer
     * Etc wait for data to be written to disk
     */
    void waitCompleted() throws IOException;

    ReadableByteChannel readFromOffset(long offset) throws IOException;

    void close() throws IOException;
}
