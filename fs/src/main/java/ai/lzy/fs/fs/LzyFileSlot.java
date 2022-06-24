package ai.lzy.fs.fs;

import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.IOException;
import java.nio.file.Path;

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
