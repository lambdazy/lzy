package ai.lzy.fs.backands;

import java.io.IOException;
import java.io.InputStream;

public interface OutputSlotBackend {
    /**
     * Blocking operation to prepare output slot data for transfer
     * Etc wait for data to be written to disk
     */
    void waitCompleted() throws IOException;

    InputStream readFromOffset(long offset) throws IOException;

    void close() throws IOException;
}
