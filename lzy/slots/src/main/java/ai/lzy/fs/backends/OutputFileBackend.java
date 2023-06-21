package ai.lzy.fs.backends;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
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
    public InputStream readFromOffset(long offset) throws IOException {
        var channel = FileChannel.open(path, StandardOpenOption.READ);
        channel.position(offset);
        return Channels.newInputStream(channel);
    }

    @Override
    public void close() throws IOException {}
}
