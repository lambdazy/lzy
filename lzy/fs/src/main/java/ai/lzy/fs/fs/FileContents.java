package ai.lzy.fs.fs;

import jnr.ffi.Pointer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

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
