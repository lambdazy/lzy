package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import jnr.ffi.Pointer;
import org.apache.log4j.Logger;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.FileContents;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class InFileSlot extends LzyInputSlotBase implements LzyFileSlot {
    private static final Logger LOG = Logger.getLogger(InFileSlot.class);

    private final Path storage;
    private final OutputStream outputStream;

    public InFileSlot(String tid, Slot definition) throws IOException {
        super(tid, definition);
        storage = Files.createTempFile("lzy", "file-slot");
        outputStream = Files.newOutputStream(storage);
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        outputStream.write(bytes.toByteArray());
    }

    @Override
    public Path location() {
        return Path.of(name());
    }

    @Override
    public long size() {
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
            return (Long)Files.getAttribute(storage, "unix:creationTime");
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public long mtime() {
        try {
            return (Long)Files.getAttribute(storage, "unix:lastModifiedTime");
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public long atime() {
        try {
            return (Long)Files.getAttribute(storage, "unix:lastAccessTime");
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public void remove() throws IOException {
        Files.delete(storage);
    }


    @Override
    public FileContents open(FuseFileInfo fi) throws IOException {
        waitForState(Operations.SlotStatus.State.CLOSED);
        SeekableByteChannel channel = Files.newByteChannel(storage);
        return new FileContents() {
            @Override
            public int read(Pointer buf, long offset, long size) throws IOException {
                final byte[] bytes = new byte[(int)size];
                channel.position(offset);
                final ByteBuffer bb = ByteBuffer.wrap(bytes);
                channel.read(bb);
                bb.flip();
                buf.put(0, bytes, 0, bb.remaining());
                return bb.remaining();
            }

            @Override
            public int write(Pointer buf, long offset, long size) throws IOException {
                throw new IOException("Attempt to write into read-only file");
            }

            @Override
            public void close() throws IOException {
                channel.close();
            }
        };
    }
}
