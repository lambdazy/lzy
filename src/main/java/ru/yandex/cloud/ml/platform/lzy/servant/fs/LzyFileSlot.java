package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.IOException;
import java.nio.file.Path;

public interface LzyFileSlot extends LzySlot {
    Path location();

    long size();

    long atime();
    long mtime();
    long ctime();

    FileContents open(FuseFileInfo fi) throws IOException;

    void remove() throws IOException;
}
