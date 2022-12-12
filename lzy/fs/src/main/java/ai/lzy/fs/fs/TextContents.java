package ai.lzy.fs.fs;

import jnr.ffi.Pointer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;

public class TextContents extends FileContentsBase {
    private final Path to;
    private final byte[] bytes;

    public TextContents(Path to, CharSequence seq) {
        this.to = to;
        bytes = seq.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int read(Pointer buf, long offset, long size) {
        final int bytesToRead = (int) Math.min(bytes.length - offset, size);
        buf.put(0, bytes, (int) offset, bytesToRead);
        trackers().forEach(t -> t.onRead(offset, ByteBuffer.wrap(bytes, 0, bytesToRead)));
        return bytesToRead;
    }

    @Override
    public int write(Pointer buf, long offset, long size) throws IOException {
        throw new AccessDeniedException(to.toAbsolutePath().toString());
    }

    @Override
    public void close() {
        trackers().forEach(ContentsTracker::onClose);
    }
}
