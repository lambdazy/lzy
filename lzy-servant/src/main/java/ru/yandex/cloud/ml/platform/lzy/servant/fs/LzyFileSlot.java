package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.io.IOException;
import java.nio.file.Path;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public interface LzyFileSlot extends LzySlot {
    Path location();

    long size();

    long atime();

    long mtime();

    long ctime();

    int mtype();

    FileContents open(FuseFileInfo fi) throws IOException;

    void remove() throws IOException;
}
