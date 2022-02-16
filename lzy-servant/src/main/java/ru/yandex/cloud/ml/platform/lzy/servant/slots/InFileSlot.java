package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.ForkJoinPool;
import jnr.ffi.Pointer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.FileContents;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.SlotConnectionManager.SlotController;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SlotSnapshotProvider;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Operations.SlotStatus.State;

public class InFileSlot extends LzyInputSlotBase implements LzyFileSlot {
    private static final Logger LOG = LogManager.getLogger(InFileSlot.class);

    private final Path storage;
    private final OutputStream outputStream;

    public InFileSlot(String tid, Slot definition, SlotSnapshotProvider snapshotProvider) throws IOException {
        this(tid, definition, Files.createTempFile("lzy", "file-slot"), snapshotProvider);
    }

    public InFileSlot(String tid, Slot definition, Path storage, SlotSnapshotProvider snapshotProvider)
        throws IOException {
        super(tid, definition, snapshotProvider);
        this.storage = storage;
        outputStream = Files.newOutputStream(storage);
    }

    @Override
    public void connect(URI slotUri, SlotController slotController) {
        ForkJoinPool.commonPool().execute(() -> {
            LOG.info("LzyInputSlotBase:: Attempt to connect to " + slotUri + " slot " + this);
            super.connect(slotUri, slotController);
            readAll();
        });
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        outputStream.write(bytes.toByteArray());
    }

    @Override
    public Path location() {
        return Path.of(URI.create(name()).getPath());
    }

    @Override
    public long size() {
        LOG.info("InFileSlot::size() for slot " + name());
        waitForState(Operations.SlotStatus.State.OPEN);
        try {
            return Files.size(storage);
        } catch (IOException e) {
            LOG.warn("Unable to get a storage file size", e);
            return 0;
        }
    }

    @Override
    public long ctime() {
        try {
            return ((FileTime) Files.getAttribute(storage, "unix:creationTime")).toMillis();
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public long mtime() {
        try {
            return ((FileTime) Files.getAttribute(storage, "unix:lastModifiedTime")).toMillis();
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public long atime() {
        try {
            return ((FileTime) Files.getAttribute(storage, "unix:lastAccessTime")).toMillis();
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    public int mtype() {
        return FileStat.S_IFREG;
    }

    @Override
    public void remove() throws IOException {
        Files.delete(storage);
    }

    @Override
    public FileContents open(FuseFileInfo fi) throws IOException {
        final SeekableByteChannel channel = Files.newByteChannel(storage);
        return new FileContents() {
            @Override
            public int read(Pointer buf, long offset, long size) throws IOException {
                if (state() != State.OPEN) {
                    try {
                        //to avoid non-stop retries which take CPU
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return -ErrorCodes.EAGAIN();
                }
                final byte[] bytes = new byte[(int) size];
                channel.position(offset);
                final ByteBuffer bb = ByteBuffer.wrap(bytes);
                int read = channel.read(bb);
                LOG.info("Read slot {} from file {}: {}", name(), storage.toString(), read);
                if (read < 0) {
                    return 0;
                }
                buf.put(0, bytes, 0, read);
                return read;
            }

            @Override
            public int write(Pointer buf, long offset, long size) throws IOException {
                throw new IOException("Attempt to write into read-only file");
            }

            @Override
            public void close() throws IOException {
                LOG.info("Closing file {} for slot {}", storage.toString(), name());
                InFileSlot.this.suspend();
                channel.close();
            }
        };
    }

    @Override
    public String toString() {
        return "InFileSlot:" + definition().name() + "->" + storage.toString();
    }
}
