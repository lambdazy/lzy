package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.util.function.Consumer;
import jnr.ffi.Pointer;

public interface FileContents extends Closeable {
    int read(Pointer buf, long offset, long size) throws IOException;
    int write(Pointer buf, long offset, long size) throws IOException;

    void track(ContentsTracker tracker);

    interface ContentsTracker {
        default void onWrite(long offset, ByteBuffer chunk) {}
        default void onRead(long offset, ByteBuffer chunk) {}
        default void onClose() {}
    }

}
