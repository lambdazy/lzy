package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import jnr.ffi.Pointer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;

public interface FileContents extends Closeable {
    int read(Pointer buf, long offset, long size) throws IOException;
    int write(Pointer buf, long offset, long size) throws IOException;

    class Text implements FileContents {
        private final Path to;
        private final byte[] bytes;

        public Text(Path to, CharSequence seq) {
            this.to = to;
            bytes = seq.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public int read(Pointer buf, long offset, long size) {
            buf.put(0, bytes, (int)offset, (int)size);
            return (int)size;
        }

        @Override
        public int write(Pointer buf, long offset, long size) throws IOException {
            throw new AccessDeniedException(to.toAbsolutePath().toString());
        }

        @Override
        public void close() {}
    }
}
