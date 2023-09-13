package ai.lzy.env.logs;

import jakarta.annotation.Nullable;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Arrays.copyOfRange;

class LogStreamQueue extends Thread {
    private static final int MAX_LOG_CHUNK_SIZE = 1 << 20; // 1Mb

    private final LinkedBlockingQueue<Input> inputs = new LinkedBlockingQueue<>();
    private final String streamName;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final List<LogWriter> writers;

    public LogStreamQueue(String streamName, List<LogWriter> writers) {
        this.writers = writers;
        this.streamName = streamName;
    }

    /**
     * Add input stream to queue
     * @return Future will be completed after writing all data from this stream
     */
    public CompletableFuture<Void> add(InputStream stream, @Nullable Function<String, String> formatter) {
        try {
            var future  = new CompletableFuture<Void>();
            inputs.put(new Input(future, stream, null, formatter));
            return future;
        } catch (InterruptedException e) {
            LogHandle.LOG.error("Error while adding stream to queue", e);
            throw new RuntimeException("Must be unreachable");
        }
    }

    /**
     * Add string for writing
     */
    public CompletableFuture<Void> add(String s) {
        try {
            var future  = new CompletableFuture<Void>();
            inputs.put(new Input(future, null, s, null));
            return future;
        } catch (InterruptedException e) {
            LogHandle.LOG.error("Error while adding stream to queue", e);
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
                if (inputHandle.formatter() != null) {
                    writeFormattedStream(inputHandle.stream(), inputHandle.formatter());
                } else {
                    writeRawStream(inputHandle.stream());
                }
            }

            if (inputHandle.string() != null) {
                try {
                    writeLines(inputHandle.string().getBytes());
                } catch (IOException e) {
                    LogHandle.LOG.warn("Cannot write buffer to stream {}: ", streamName, e);
                }
            }

            inputHandle.future.complete(null);
        }

        writeEos();
    }

    private void writeFormattedStream(InputStream stream, Function<String, String> formatter) {
        try (stream) {
            var iterator = IOUtils.lineIterator(stream, StandardCharsets.UTF_8);

            while (iterator.hasNext()) {
                var line = iterator.nextLine();
                line = formatter.apply(line);

                if (line.length() > MAX_LOG_CHUNK_SIZE) {
                    line = line.substring(MAX_LOG_CHUNK_SIZE / 2) + " ... TRUNCATED ...";
                }

                writeLines(line.getBytes());
            }
        } catch (IOException e) {
            LogHandle.LOG.error("Error while writing to stream {}: ", streamName, e);
        }
    }

    private void writeRawStream(InputStream stream) {
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
                    writeLinesWithLimits(buf, linesPos, n + 1);
                    srcPos += readLen;
                    linesPos = n + 1;
                }
            }

            if (srcPos > linesPos) {
                writeLinesWithLimits(buf, linesPos, srcPos);
            }

        } catch (IOException e) {
            LogHandle.LOG.error("Error while writing to stream {}: ", streamName, e);
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

    private void writeLinesWithLimits(byte[] buf, int from, int to) throws IOException {
        var len = to - from;
        if (len <= MAX_LOG_CHUNK_SIZE) {
            writeLines(copyOfRange(buf, from, to));
            return;
        }

        final byte[] truncatedSuffix = " ... TRUNCATED ...\n".getBytes();

        int i = from;
        for (; i < to; ++i) {
            if (buf[i] == '\n' || i == to - 1) {
                if (i + 1 - from <= MAX_LOG_CHUNK_SIZE) {
                    writeLines(copyOfRange(buf, from, i + 1));
                } else {
                    var prefix = copyOfRange(buf, from, from + MAX_LOG_CHUNK_SIZE - truncatedSuffix.length);
                    System.arraycopy(truncatedSuffix, 0, prefix,
                        prefix.length - truncatedSuffix.length, truncatedSuffix.length);
                    writeLines(prefix);
                }
                from = i + 1;
            }
        }
    }

    private void writeLines(byte[] lines) throws IOException {
        if (LogHandle.LOG.isDebugEnabled()) {
            LogHandle.LOG.debug("[{}]: {}", streamName, new String(lines));
        }

        for (var writer: writers) {
            try {
                writer.writeLines(streamName, lines);
            } catch (Exception e) {
                LogHandle.LOG.warn("Error while writing logs to logsWriter: ", e);
            }
        }
    }

    private void writeEos() {
        for (var writer: writers) {
            try {
                writer.writeEos(streamName);
            } catch (Exception e) {
                LogHandle.LOG.warn("Error while writing logs to logsWriter: ", e);
            }
        }
    }

    /**
     * Interrupt thread and wait for all data to be written
     */
    public void close() throws InterruptedException {
        LogHandle.LOG.info("Closing stream {}", streamName);
        this.stopping.set(true);
        this.interrupt();
        this.join();
    }

    private record Input(
        CompletableFuture<Void> future,
        @Nullable InputStream stream,
        @Nullable String string,
        @Nullable Function<String, String> formatter
    ) {}

}
