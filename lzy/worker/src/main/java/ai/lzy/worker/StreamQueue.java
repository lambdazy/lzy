package ai.lzy.worker;

import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamQueue extends Thread {
    private final OutputStream out;
    private final LinkedBlockingQueue<InputStream> inputs = new LinkedBlockingQueue<>();
    private final Logger logger;
    private final String streamName;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    public StreamQueue(OutputStream out, Logger log, String streamName) {
        this.out = out;
        this.logger = log;
        this.streamName = streamName;
    }

    public void add(InputStream stream) {
        try {
            inputs.put(stream);
        } catch (InterruptedException e) {
            logger.error("Error while adding stream to queue", e);
            throw new RuntimeException("Must be unreachable");
        }
    }

    @Override
    public void run() {
        while (!(stopping.get() && inputs.isEmpty())) {
            final InputStream input;
            try {
                input = this.inputs.take();
            } catch (InterruptedException e) {
                continue;
            }
            try {
                var buf = new byte[4096];
                var len = 0;
                while ((len = input.read(buf)) != -1) {
                    if (logger.isDebugEnabled()) {
                        var msg = Arrays.copyOfRange(buf, 0, len);
                        logger.debug("[{}]: {}", streamName, new String(msg));
                    }
                    out.write(buf, 0, len);
                }
            } catch (IOException e) {
                logger.error("Error while writing to stream {}: ", streamName, e);
                return;
            }
        }
        try {
            out.close();
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
}
