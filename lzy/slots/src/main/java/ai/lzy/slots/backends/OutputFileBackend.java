package ai.lzy.slots.backends;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class OutputFileBackend implements OutputSlotBackend {
    private final Path path;

    public OutputFileBackend(Path path) {
        this.path = path;

        if (!path.toFile().exists()) {
            throw new RuntimeException("File not exists");
        }
    }

    @Override
    public void waitCompleted() {}  // Already completed

    @Override
    public ReadableByteChannel readFromOffset(long offset) throws IOException {
        var channel = FileChannel.open(path, StandardOpenOption.READ);
        channel.position(offset);
        return channel;
    }

    @Override
    public void close() throws IOException {}
}
