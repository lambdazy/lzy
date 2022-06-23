package ai.lzy.fs.snapshot;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class StreamsWrapper {
    private final InputStream wrappedInputStream;
    private final OutputStream wrappedOutputStream;
    private final Future<?> future;

    public StreamsWrapper(InputStream inputStream, OutputStream outputStream, Future<?> future) {
        wrappedInputStream = inputStream;
        wrappedOutputStream = outputStream;
        this.future = future;
    }

    public synchronized void write(ByteString b) {
        try {
            wrappedOutputStream.write(b.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("StreamsWrapper::write exception while writing to output stream");
        }
    }

    public synchronized void write(InputStream b) {
        try {
            wrappedOutputStream.write(b.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException("StreamsWrapper::write exception while writing to output stream");
        }
    }

    public synchronized void close() {
        try {
            wrappedOutputStream.close();
            future.get();
            wrappedInputStream.close();
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
