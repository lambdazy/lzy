package ai.lzy.env.logs;

import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.copyOfRange;

class LogStreamQueue extends Thread {
    private final LinkedBlockingQueue<Input> inputs = new LinkedBlockingQueue<>();
    private final Logger logger;
    private final String streamName;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final List<LogWriter> writers;

    public LogStreamQueue(String streamName, List<LogWriter> writers, Logger logger) {
        this.writers = writers;
        this.streamName = streamName;
        this.logger = logger;
    }

    /**
     * Add input stream to queue
     * @return Future will be completed after writing all data from this stream
     */
    public CompletableFuture<Void> add(InputStream stream) {
        try {
            var future  = new CompletableFuture<Void>();
            inputs.put(new Input(future, stream, null));
            return future;
        } catch (InterruptedException e) {
            logger.error("Error while adding stream to queue", e);
            throw new RuntimeException("Must be unreachable");
        }
    }

    /**
     * Add string for writing
     */
    public CompletableFuture<Void> add(String s) {
        try {
            var future  = new CompletableFuture<Void>();
            inputs.put(new Input(future, null, s));
            return future;
        } catch (InterruptedException e) {
            logger.error("Error while adding stream to queue", e);
            throw new RuntimeException("Must be unreachable");
        }
    }

    @Override
    public void run() {
        while (!(stopping.get() && inputs.isEmpty())) {
            final Input inputHandle;
            try {
                inputHandle = this.inputs.take();
            } catch (InterruptedException e) {
                //ignored
                continue;
            }

            //noinspection resource
            if (inputHandle.stream() != null) {
                writeStream(inputHandle.stream());
            }

            if (inputHandle.string() != null) {
                try {
                    writeLines(inputHandle.string().getBytes());
                } catch (IOException e) {
                    logger.warn("Cannot write buffer to stream {}: ", streamName, e);
                }
            }

            inputHandle.future.complete(null);
        }

        writeEos();
    }

    private void writeStream(InputStream stream) {
        final int initialSize = 4096;
        final int gap = 128;
        try (final var input = stream) {
            var buf = new byte[initialSize];
            int srcPos = 0;
            int linesPos = 0;
            int readLen = 0;

            while ((readLen = input.read(buf, srcPos, buf.length - srcPos)) != -1) {
                int n = findLastIndexOf(buf, (byte) '\n', srcPos, srcPos + readLen - 1);
                if (n == -1) {
                    srcPos += readLen;
                    if (srcPos + gap > buf.length) {
                        if (srcPos - linesPos + gap > buf.length) {
                            var tmp = buf;
                            buf = new byte[(int) (tmp.length * 1.5)];
                            System.arraycopy(tmp, 0, buf, 0, tmp.length);
                        } else {
                            System.arraycopy(buf, linesPos, buf, 0, srcPos - linesPos);
                            srcPos = srcPos - linesPos;
                            linesPos = 0;
                        }
                    }
                } else {
                    writeLines(copyOfRange(buf, linesPos, n + 1));
                    srcPos += readLen;
                    linesPos = n + 1;
                }
            }

            if (srcPos > linesPos) {
                writeLines(copyOfRange(buf, linesPos, srcPos));
            }

        } catch (IOException e) {
            logger.error("Error while writing to stream {}: ", streamName, e);
        }
    }

    private static int findLastIndexOf(byte[] buf, byte ch, int from, int to) {
        for (int i = to; i >= from; --i) {
            if (buf[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    private void writeLines(byte[] lines) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}]: {}", streamName, new String(lines));
        }

        for (var writer: writers) {
            try {
                writer.writeLines(streamName, lines);
            } catch (Exception e) {
                logger.warn("Error while writing logs to logsWriter: ", e);
            }
        }
    }

    private void writeEos() {
        for (var writer: writers) {
            try {
                writer.writeEos(streamName);
            } catch (Exception e) {
                logger.warn("Error while writing logs to logsWriter: ", e);
            }
        }
    }

    /**
     * Interrupt thread and wait for all data to be written
     */
    public void close() throws InterruptedException {
        logger.info("Closing stream {}", streamName);
        this.stopping.set(true);
        this.interrupt();
        this.join();
    }

    private record Input(
        CompletableFuture<Void> future,
        @Nullable InputStream stream,
        @Nullable String string
    ) {}

}
