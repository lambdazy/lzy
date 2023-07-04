package ai.lzy.fs.transfers;

import java.nio.channels.ReadableByteChannel;

public interface OutputTransfer extends AutoCloseable {

    /**
     * Blocking operation to transfer data from reader
     */
    void readFrom(ReadableByteChannel source);

    @Override
    default void close() {}
}
