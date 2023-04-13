package ai.lzy.worker;

import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.common.LMO;
import jakarta.annotation.Nullable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.FormattedMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.copyOfRange;

public class StreamQueue extends Thread {
    private final ArrayList<OutputStream> outs = new ArrayList<>();
    private final LinkedBlockingQueue<Input> inputs = new LinkedBlockingQueue<>();
    private final Logger logger;
    private final String streamName;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    @Nullable
    private final KafkaProducer<String, byte[]> kafkaClient;
    @Nullable
    private final String topic;

    public StreamQueue(@Nullable LMO.KafkaTopicDescription topic, Logger log, String streamName,
                       @Nullable KafkaHelper helper)
    {
        this.logger = log;
        this.streamName = streamName;
        if (topic != null && helper != null) {
            var props = helper.toProperties(topic.getUsername(), topic.getPassword());

            this.kafkaClient = new KafkaProducer<>(props);
            this.topic = topic.getTopic();
        } else {
            this.kafkaClient = null;
            this.topic = null;
        }
    }

    /**
     * Add input stream to queue
     * @return Future will be completed after writing all data from this stream
     */
    public CompletableFuture<Void> add(String taskId, InputStream stream) {
        try {
            var future  = new CompletableFuture<Void>();
            inputs.put(new Input(taskId, future, stream, null));
            return future;
        } catch (InterruptedException e) {
            logger.error("Error while adding stream to queue", e);
            throw new RuntimeException("Must be unreachable");
        }
    }

    /**
     * Add string for writing
     */
    public CompletableFuture<Void> add(String taskId, String s) {
        try {
            var future  = new CompletableFuture<Void>();
            inputs.put(new Input(taskId, future, null, s));
            return future;
        } catch (InterruptedException e) {
            logger.error("Error while adding stream to queue", e);
            throw new RuntimeException("Must be unreachable");
        }
    }

    /**
     * Adds output stream to write to
     */
    public void add(OutputStream stream) {
        synchronized (outs) {
            outs.add(stream);
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
                writeStream(inputHandle.taskId(), inputHandle.stream());
            }

            if (inputHandle.string() != null) {
                try {
                    writeLines(inputHandle.taskId(), inputHandle.string().getBytes());
                } catch (IOException e) {
                    logger.warn("Cannot write buffer to stream {}: ", streamName, e);
                }
            }

            inputHandle.future.complete(null);
        }
        try {
            synchronized (outs) {
                for (var out: outs) {
                    out.close();
                }
            }
            if (kafkaClient != null) {
                kafkaClient.close();
            }
        } catch (IOException e) {
            logger.error("Error while closing out slot for stream {}: ", streamName, e);
            throw new RuntimeException(e);
        }
    }

    private void writeStream(String taskId, InputStream stream) {
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
                    writeLines(taskId, copyOfRange(buf, linesPos, n + 1));
                    srcPos += readLen;
                    linesPos = n + 1;
                }
            }

            if (srcPos > linesPos) {
                writeLines(taskId, copyOfRange(buf, linesPos, srcPos - linesPos));
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

    private void writeLines(String taskId, byte[] lines) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}]: {}", streamName, new String(lines));
        }
        synchronized (outs) {
            for (var out : outs) {
                out.write(lines, 0, lines.length);
            }
        }
        if (kafkaClient != null && topic != null) {
            // Using single partition to manage global order of logs !!!
            try {
                var headers = new RecordHeaders();
                headers.add("stream", streamName.getBytes(StandardCharsets.UTF_8));

                kafkaClient.send(new ProducerRecord<>(topic, /* partition */ 0, taskId, lines, headers)).get();
            } catch (Exception e) {
                logger.warn("Cannot send data to kafka: ", e);
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
        String taskId,
        CompletableFuture<Void> future,
        @Nullable InputStream stream,
        @Nullable String string
    ) {}

    public static class LogHandle implements AutoCloseable {
        private static final String PREFIX = "[SYS] ";
        private final Logger logger;
        private final StreamQueue outQueue;
        private final StreamQueue errQueue;
        private final ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();


        public LogHandle(StreamQueue outQueue, StreamQueue errQueue, Logger logger) {
            this.logger = logger;
            this.outQueue = outQueue;
            this.errQueue = errQueue;
        }

        public static LogHandle fromTopicDesc(Logger logger, @Nullable LMO.KafkaTopicDescription topicDesc,
                                              @Nullable KafkaHelper helper)
        {
            var outQueue = new StreamQueue(topicDesc, logger, "out", helper);
            outQueue.start();

            var errQueue = new StreamQueue(topicDesc, logger, "err", helper);
            errQueue.start();

            return new LogHandle(outQueue, errQueue, logger);
        }

        public void logOut(String taskId, String pattern, Object... values) {
            var formatted  = PREFIX + new FormattedMessage(pattern, values) + "\n";

            logger.info(formatted);
            futures.add(outQueue.add(taskId, formatted));
        }

        public CompletableFuture<Void> logOut(String taskId, InputStream stream) {
            var fut = outQueue.add(taskId, stream);
            futures.add(fut);
            return fut;
        }

        public void logErr(String taskId, String pattern, Object... values) {
            var formatted  = PREFIX + new FormattedMessage(pattern, values) + "\n";

            logger.info(formatted);
            futures.add(errQueue.add(taskId, formatted));
        }

        public CompletableFuture<Void> logErr(String taskId, InputStream stream) {
            var fut = errQueue.add(taskId, stream);
            futures.add(fut);
            return fut;
        }

        public void addErrOutput(OutputStream stream) {
            errQueue.add(stream);
        }

        public void addOutOutput(OutputStream stream) {
            outQueue.add(stream);
        }

        @Override
        public void close() {
            try {
                for (var fut: futures) {
                    fut.get();
                }
                outQueue.close();
                errQueue.close();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Cannot close out/error streams: ", e);
            }
        }
    }
}
