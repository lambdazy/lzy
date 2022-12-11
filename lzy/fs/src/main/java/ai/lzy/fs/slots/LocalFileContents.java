package ai.lzy.fs.slots;

import ai.lzy.fs.fs.FileContentsBase;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.ErrorCodes;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;

import static java.nio.file.StandardOpenOption.WRITE;

public class LocalFileContents extends FileContentsBase {
    private final byte[] buffer = new byte[4096];
    private final FileChannel channel;
    private final boolean writable;

    public LocalFileContents(Path file, OpenOption... oo) throws IOException {
        channel = FileChannel.open(file, oo);
        writable = Arrays.stream(oo).anyMatch(openOption -> openOption == WRITE);
    }

    @Override
    public int read(Pointer buf, long offset, long size) throws IOException {
        final long bytesToRead = Math.min(channel.size() - offset, size);
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, offset, bytesToRead);
        buf.transferFrom(0, Pointer.wrap(buf.getRuntime(), map), 0, bytesToRead);
        trackers().forEach(tracker -> tracker.onRead(offset, map));
        return (int) bytesToRead;
    }

    @Override
    public int write(Pointer buf, long offset, long size) throws IOException {
        if (!writable) {
            return -ErrorCodes.EROFS();
        }

        int off = 0;
        channel.position(offset);
        while (off < size) {
            int chunkSize = (int) Math.min(buffer.length, size - off);
            buf.get(off, buffer, 0, chunkSize);
            final ByteBuffer wrap = ByteBuffer.wrap(buffer);
            wrap.limit(chunkSize);
            while (wrap.remaining() > 0) {
                final int ww = channel.write(wrap);
                if (ww < 0) {
                    throw new EOFException();
                }
                final long effectiveOffset = off + offset;
                trackers().forEach(t -> t.onWrite(effectiveOffset, ByteBuffer.wrap(this.buffer, 0, ww)));
            }
            off += chunkSize;
        }
        return off;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        trackers().forEach(ContentsTracker::onClose);
    }
}
