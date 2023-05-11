package ai.lzy.fs.slots;

import ai.lzy.fs.fs.FileContents;
import ai.lzy.fs.fs.FileContentsBase;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMS.SlotStatus.State;
import com.google.protobuf.ByteString;
import jnr.ffi.Pointer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.stream.Stream;

public class InFileSlot extends LzyInputSlotBase implements LzyFileSlot {
    private static final Logger LOG = LogManager.getLogger(InFileSlot.class);
    private static final ThreadGroup READER_TG = new ThreadGroup("input-slot-readers");

    private final Path storage;
    private final OutputStream outputStream;
    private final boolean allowMultipleRead;
    private int readCount;

    public InFileSlot(SlotInstance instance, Path storage, boolean allowMultipleRead) throws IOException {
        super(instance);
        this.storage = storage;
        outputStream = Files.newOutputStream(storage);
        this.allowMultipleRead = allowMultipleRead;
        this.readCount = 0;
    }

    public InFileSlot(SlotInstance instance, Path storage) throws IOException {
        this(instance, storage, true);
    }

    public InFileSlot(SlotInstance instance) throws IOException {
        this(instance, Files.createTempFile("lzy", "file-slot"));
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> dataProvider) {
        super.connect(slotUri, dataProvider);
        LOG.info("Attempt to connect to " + slotUri + " slot " + this);
        Thread t = new Thread(READER_TG, this::readAll, "reader-from-" + slotUri + "-to-" + definition().name());
        t.start();
        onState(State.DESTROYED, t::interrupt);
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        super.onChunk(bytes);
        outputStream.write(bytes.toByteArray());
    }

    @Override
    public Path location() {
        return Path.of(URI.create(name()).getPath());
    }

    @Override
    public long size() {
        LOG.info("InFileSlot::size() for slot " + name());
        waitForState(LMS.SlotStatus.State.OPEN);
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
        return new FileContentsBase() {
            @Override
            public int read(Pointer buf, long offset, long size) throws IOException {
                if (state() != State.OPEN) {
                    if (state() == State.DESTROYED) {
                        return -ErrorCodes.EIO();
                    }
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
                trackers().forEach(tracker -> tracker.onRead(offset, ByteBuffer.wrap(bytes)));
                int read = channel.read(bb);
                readCount++;
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
                channel.close();
                trackers().forEach(ContentsTracker::onClose);
                if (!allowMultipleRead) {
                    state(State.SUSPENDED);
                } else {
                    if (readCount > 1) {
                        LOG.debug("Repeated reading for slot {}", name());
                    }
                }
            }
        };
    }

    @Override
    public String toString() {
        return "InFileSlot:" + definition().name() + "->" + storage.toString();
    }
}
