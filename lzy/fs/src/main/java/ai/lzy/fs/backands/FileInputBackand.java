package ai.lzy.fs.backands;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileInputBackand implements InputSlotBackend {
    private final Path path;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FileInputBackand(Path path) throws IOException {
        this.path = path;

        if (!path.toFile().exists()) {
            path.toFile().createNewFile();
        } else {
            path.toFile().delete();
            path.toFile().createNewFile();
        }
    }

    @Override
    public OutputStream outputStream() throws IOException {
        if (closed.get()) {
            throw new IOException("Already closed");
        }
        return new FileOutputStream(path.toFile());
    }

    @Override
    public synchronized OutputSlotBackend toOutput() {
        if (closed.get()) {
            throw new RuntimeException("Already closed");
        }

        closed.set(true);
        return new OutputFileBackend(path);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed.get()) {
            return;
        }

        path.toFile().delete();
        closed.set(true);
    }
}
