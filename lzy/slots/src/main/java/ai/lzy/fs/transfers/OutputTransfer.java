package ai.lzy.fs.transfers;

import java.io.InputStream;

public interface OutputTransfer extends AutoCloseable {

    /**
     * Blocking operation to transfer data from reader
     */
    void readFrom(InputStream source);

    @Override
    default void close() {}
}
