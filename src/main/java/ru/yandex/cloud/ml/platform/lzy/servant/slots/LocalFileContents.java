package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import jnr.ffi.Pointer;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.FileContents;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LocalFileContents implements FileContents {
    private final byte[] buffer = new byte[4096];
    private final FileChannel channel;

    public LocalFileContents(Path file, OpenOption... oo) throws IOException {
        channel = FileChannel.open(file, oo);
    }

    @Override
    public int read(Pointer buf, long offset, long size) throws IOException {
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, offset, size);
        buf.transferFrom(0, Pointer.wrap(buf.getRuntime(), map), 0, size);
        return (int)size;
    }

    @Override
    public int write(Pointer buf, long offset, long size) throws IOException {
        int off = 0;
        while (off < size) {
            int chunkSize = (int)Math.min(buffer.length, size - off);
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
        return off;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        triggers.forEach(Runnable::run);
    }

    private final List<Runnable> triggers = new ArrayList<>();
    public void onClose(Runnable trigger) {
        triggers.add(trigger);
    }
}
