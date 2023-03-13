package ai.lzy.logs;

import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.headers.LH;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.FormattedMessage;
import org.apache.logging.log4j.util.Strings;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import static java.util.Arrays.copyOfRange;

public class StreamQueue extends Thread {
    private final ArrayList<OutputStream> outs = new ArrayList<>();
    private final LinkedBlockingQueue<Input> inputs = new LinkedBlockingQueue<>();
    private final Logger logger;
    private final String streamName;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    @Nullable private final KafkaProducer<String, byte[]> kafkaClient;
    @Nullable private final String topic;

    public StreamQueue(@Nullable LH.UserLogsHeader topic, Logger log, String streamName) {
        this.logger = log;
        this.streamName = streamName;
        if (topic != null && topic.hasKafkaTopicDesc()) {
            var kafkaDesc = topic.getKafkaTopicDesc();
            var props = new Properties();
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("bootstrap.servers", Strings.join(kafkaDesc.getBootstrapServersList(), ','));
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "  username=\"" + kafkaDesc.getUsername() + "\"" +
                "  password=\"" + kafkaDesc.getPassword() + "\";");

            this.kafkaClient = new KafkaProducer<>(props);
            this.topic = kafkaDesc.getTopic();
        } else {
            kafkaClient = null;
            this.topic = null;
        }
    }

    /**
     * Add input stream to queue
     * @return Future will be completed after writing all data from this stream
     */
    public CompletableFuture<Void> add(InputStream stream) {
        try {
            var future  = new CompletableFuture<Void>();
            inputs.put(new Input(future, stream));
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
            try (final var input = inputHandle.stream()) {
                var buf = new byte[4096];
                var len = 0;
                while ((len = input.read(buf)) != -1) {
                    if (logger.isDebugEnabled()) {
                        var msg = copyOfRange(buf, 0, len);
                        logger.debug("[{}]: {}", streamName, new String(msg));
                    }
                    synchronized (outs) {
                        for (var out : outs) {
                            out.write(buf, 0, len);
                        }
                    }
                    if (kafkaClient != null && topic != null) {
                        // Using single partition to manage global order of logs
                        try {
                            kafkaClient.send(new ProducerRecord<>(topic, 0, streamName, copyOfRange(buf, 0, len)))
                                .get();
                        } catch (Exception e) {
                            logger.warn("Cannot send data to kafka: ", e);
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("Error while writing to stream {}: ", streamName, e);
                return;
            } finally {
                inputHandle.future.complete(null);
            }
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

    /**
     * Interrupt thread and wait for all data to be written
     */
    public void close() throws InterruptedException {
        logger.info("Closing stream {}", streamName);
        this.stopping.set(true);
        this.interrupt();
        this.join();
    }

    private record Input(CompletableFuture<Void> future, InputStream stream) {}

    public static class LogHandle implements AutoCloseable {
        private static final String PREFIX = "[SYS] ";

        private OutputStream out;
        private OutputStream err;
        private final Logger logger;
        private final StreamQueue outQueue;
        private final StreamQueue errQueue;
        private final ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();


        public LogHandle(StreamQueue outQueue, StreamQueue errQueue, Logger logger) {
            this.logger = logger;
            out = new PipedOutputStream();
            err = new PipedOutputStream();
            this.outQueue = outQueue;
            this.errQueue = errQueue;

            try {
                futures.add(outQueue.add(new PipedInputStream((PipedOutputStream) out)));
            } catch (IOException e) {
                logger.error("Cannot init io stream for stdout, running without user logs");

                try {
                    out.close();
                } catch (IOException ex) {
                    // ignored
                }

                out = OutputStream.nullOutputStream();
            }

            try {
                futures.add(errQueue.add(new PipedInputStream((PipedOutputStream) err)));
            } catch (IOException e) {
                logger.error("Cannot init io stream for stderr, running without user logs");

                try {
                    err.close();
                } catch (IOException ex) {
                    // ignored
                }

                err = OutputStream.nullOutputStream();
            }
        }

        public static LogHandle fromHeaders(Logger logger) {
            var logHeader = GrpcHeaders.getHeader(GrpcHeaders.USER_LOGS_HEADER_KEY);

            var outQueue = new StreamQueue(logHeader, logger, "out");
            outQueue.start();

            var errQueue = new StreamQueue(logHeader, logger, "err");
            errQueue.start();

            return new LogHandle(outQueue, errQueue, logger);
        }

        public void logOut(String pattern, Object... values) {
            var formatted  = PREFIX + new FormattedMessage(pattern, values) + "\n";

            logger.info(formatted);

            try {
                out.write(formatted.getBytes());
            } catch (IOException e) {
                logger.error("Cannot write into stdout: ", e);
            }
        }

        public CompletableFuture<Void> logOut(InputStream stream) {
            var fut = outQueue.add(stream);
            futures.add(fut);
            return fut;
        }

        public void logErr(String pattern, Object... values) {
            var formatted  = PREFIX + new FormattedMessage(pattern, values) + "\n";

            logger.info(formatted);

            try {
                err.write(formatted.getBytes());
            } catch (IOException e) {
                logger.error("Cannot write into stderr: ", e);
            }
        }

        public CompletableFuture<Void> logErr(InputStream stream) {
            var fut = errQueue.add(stream);
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
                out.close();
                err.close();
                for (var fut: futures) {
                    fut.get();
                }
                outQueue.close();
                errQueue.close();
            } catch (IOException | InterruptedException | ExecutionException e) {
                logger.error("Cannot close out/error streams: ", e);
            }
        }
    }
}
