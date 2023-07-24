package ai.lzy.env.logs;

import java.io.IOException;

public interface LogWriter {
    /**
     * Write one chunk to log stream
     *
     * @param streamName Name of stream to write to (out, err, ...)
     * @param lines      Chunk to write
     */
    void writeLines(String streamName, byte[] lines) throws IOException;

    /**
     * Notify stream about EOS
     *
     * @param streamName Name of stream to write to (out, err, ...)
     */
    void writeEos(String streamName) throws IOException;
}
