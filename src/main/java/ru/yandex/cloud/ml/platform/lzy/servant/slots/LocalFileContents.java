package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import jnr.ffi.Pointer;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.FileContents;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LocalFileContents implements FileContents {
    private final FileChannel channel;

    public LocalFileContents(Path file) throws IOException {
        channel = FileChannel.open(file);
    }

    @Override
    public int read(Pointer buf, long offset, long size) throws IOException {
        return (int)channel.transferTo(offset, size, new WritableByteChannel() {
            long offset = 0;
            @Override
            public int write(ByteBuffer src) {
                Pointer.wrap(buf.getRuntime(), src);
                final int remaining = src.remaining();
                buf.transferFrom(offset, Pointer.wrap(buf.getRuntime(), src), src.position(), remaining);
                offset += remaining;
                return remaining;
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() {}
        });
    }

    @Override
    public int write(Pointer buf, long offset, long size) throws IOException {
        return (int)channel.transferFrom(new ReadableByteChannel() {
            long offset = 0;
            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() {}

            @Override
            public int read(ByteBuffer dst) {
                final int remaining = dst.remaining();
                buf.transferTo(offset, Pointer.wrap(buf.getRuntime(), dst), dst.position(), remaining);
                offset += remaining;
                return remaining;
            }
        }, offset, size);
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
