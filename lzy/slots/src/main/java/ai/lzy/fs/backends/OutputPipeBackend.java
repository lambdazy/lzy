package ai.lzy.fs.backends;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

public class OutputPipeBackend implements OutputSlotBackend {
    public static final Logger LOG = LogManager.getLogger(OutputPipeBackend.class);

    private final Path pipePath;
    private final File storageFile;
    private final AtomicBoolean isReady = new AtomicBoolean(false);

    public OutputPipeBackend(Path pipePath) throws IOException, InterruptedException {
        this.pipePath = pipePath;
        storageFile = File.createTempFile("storage", ".tmp");

        if (!pipePath.getParent().toFile().exists()) {
            Files.createDirectories(pipePath.getParent());
        }

        // Creating named pipe
        var res = Runtime.getRuntime().exec(new String[]{"mkfifo", pipePath.toAbsolutePath().toString()}).waitFor();

        if (res != 0) {
            throw new RuntimeException("Failed to create named pipe");
        }
    }

    @Override
    public synchronized void waitCompleted() throws IOException { // Synchronized to prevent multiple writing
        if (isReady.get()) {
            return;
        }

        try (var is = new FileInputStream(pipePath.toFile()); var os = new FileOutputStream(storageFile)) {
            IOUtils.copyLarge(is, os);
        }

        isReady.set(true);
    }

    @Override
    public ReadableByteChannel readFromOffset(long offset) throws IOException {
        assert isReady.get();  // Must be called only after data is ready

        var channel = FileChannel.open(storageFile.toPath(), StandardOpenOption.READ);
        channel.position(offset);
        return channel;
    }

    @Override
    public void close() throws IOException {
        storageFile.delete();
        pipePath.toFile().delete();
    }
}
