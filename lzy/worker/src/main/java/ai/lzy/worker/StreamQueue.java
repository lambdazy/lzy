package ai.lzy.worker;

import ai.lzy.v1.common.LMO;
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

    public StreamQueue(@Nullable LMO.KafkaTopicDescription topic, Logger log, String streamName) {
        this.logger = log;
        this.streamName = streamName;
        if (topic != null) {
            var props = new Properties();
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("bootstrap.servers", Strings.join(topic.getBootstrapServersList(), ','));
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "  username=\"" + topic.getUsername() + "\"" +
                "  password=\"" + topic.getPassword() + "\";");

            this.kafkaClient = new KafkaProducer<>(props);
            this.topic = topic.getTopic();
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

            if (inputHandle.stream() != null) {
                writeStream(inputHandle.stream());
            }

            if (inputHandle.string() != null) {
                try {
                    writeBuf(inputHandle.string().getBytes(), inputHandle.string().getBytes().length);
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

    private void writeStream(InputStream stream) {
        try (final var input = stream) {
            var buf = new byte[4096];
            var len = 0;
            while ((len = input.read(buf)) != -1) {
                writeBuf(buf, len);
            }
        } catch (IOException e) {
            logger.error("Error while writing to stream {}: ", streamName, e);
        }
    }

    private void writeBuf(byte[] buf, int len) throws IOException {
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

    /**
     * Interrupt thread and wait for all data to be written
     */
    public void close() throws InterruptedException {
        logger.info("Closing stream {}", streamName);
        this.stopping.set(true);
        this.interrupt();
        this.join();
    }

    private record Input(CompletableFuture<Void> future, @Nullable InputStream stream, @Nullable String string) {}

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

        public static LogHandle fromTopicDesc(Logger logger, @Nullable LMO.KafkaTopicDescription topicDesc) {
            var outQueue = new StreamQueue(topicDesc, logger, "out");
            outQueue.start();

            var errQueue = new StreamQueue(topicDesc, logger, "err");
            errQueue.start();

            return new LogHandle(outQueue, errQueue, logger);
        }

        public void logOut(String pattern, Object... values) {
            var formatted  = PREFIX + new FormattedMessage(pattern, values) + "\n";

            logger.info(formatted);
            futures.add(outQueue.add(formatted));
        }

        public CompletableFuture<Void> logOut(InputStream stream) {
            var fut = outQueue.add(stream);
            futures.add(fut);
            return fut;
        }

        public void logErr(String pattern, Object... values) {
            var formatted  = PREFIX + new FormattedMessage(pattern, values) + "\n";

            logger.info(formatted);
            futures.add(errQueue.add(formatted));
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
