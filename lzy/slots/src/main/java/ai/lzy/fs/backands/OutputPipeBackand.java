package ai.lzy.fs.backands;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class OutputPipeBackand implements OutputSlotBackend {
    public static final Logger LOG = LogManager.getLogger(OutputPipeBackand.class);

    private final Path pipePath;
    private final File storageFile;
    private final AtomicBoolean isReady = new AtomicBoolean(false);

    public OutputPipeBackand(Path pipePath) throws IOException {
        this.pipePath = pipePath;
        storageFile = File.createTempFile("storage", ".tmp");

        // Creating named pipe
        Runtime.getRuntime().exec(new String[]{"mkfifo", pipePath.toAbsolutePath().toString()});
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
    public InputStream readFromOffset(long offset) throws IOException {
        assert isReady.get();  // Must be called only after data is ready

        var fd = FileChannel.open(storageFile.toPath(), StandardOpenOption.READ);

        fd.position(offset);
        return Channels.newInputStream(fd);
    }

    @Override
    public void close() throws IOException {
        storageFile.delete();
        pipePath.toFile().delete();
    }
}
