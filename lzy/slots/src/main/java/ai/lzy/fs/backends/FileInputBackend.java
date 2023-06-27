package ai.lzy.fs.backends;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileInputBackend implements InputSlotBackend {
    private final Path path;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FileInputBackend(Path path) throws IOException {
        this.path = path;

        if (!path.toFile().exists()) {
            if (!path.getParent().toFile().exists()) {
                Files.createDirectories(path.getParent());
            }
            path.toFile().createNewFile();
        } else {
            path.toFile().delete();
            path.toFile().createNewFile();
        }
    }

    @Override
    public SeekableByteChannel openChannel() throws IOException {
        if (closed.get()) {
            throw new IOException("Already closed");
        }
        return FileChannel.open(path, StandardOpenOption.WRITE);
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
