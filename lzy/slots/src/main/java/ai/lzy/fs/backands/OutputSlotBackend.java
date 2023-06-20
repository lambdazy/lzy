package ai.lzy.fs.backands;


import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channel;
import java.util.concurrent.CompletableFuture;

public interface OutputSlotBackend {
    /**
     * Blocking operation to prepare output slot data for transfer
     * Etc wait for data to be written to disk
     */
    void waitCompleted() throws IOException;

    InputStream readFromOffset(long offset) throws IOException;

    void close() throws IOException;
}
