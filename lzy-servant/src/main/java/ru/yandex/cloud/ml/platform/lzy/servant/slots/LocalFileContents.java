package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import static java.nio.file.StandardOpenOption.WRITE;

import java.util.Arrays;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.ErrorCodes;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.FileContents;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class LocalFileContents implements FileContents {

    private final byte[] buffer = new byte[4096];
    private final FileChannel channel;
    private final AtomicBoolean written = new AtomicBoolean(false);
    private final boolean writable;

    public LocalFileContents(Path file, OpenOption... oo) throws IOException {
        channel = FileChannel.open(file, oo);
        writable = Arrays.stream(oo).anyMatch(openOption -> openOption == WRITE);
    }

    @Override
    public int read(Pointer buf, long offset, long size) throws IOException {
        final long bytesToRead = Math.min(channel.size() - offset, size);
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, offset,
            bytesToRead);
        buf.transferFrom(0, Pointer.wrap(buf.getRuntime(), map), 0, bytesToRead);
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
            }
            off += chunkSize;
        }
        written.set(true);
        return off;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        triggers.forEach(booleanConsumer -> booleanConsumer.accept(written.get()));
    }

    private final List<Consumer<Boolean>> triggers = new ArrayList<>();

    public void onClose(Consumer<Boolean> trigger) {
        triggers.add(trigger);
    }
}
