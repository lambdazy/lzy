package ai.lzy.fs.fs;

import java.nio.file.Path;

public interface LzyFSManager {
    void mount(Path mountPoint);

    void umount();

    void addSlot(LzyFileSlot slot);

    void removeSlot(String name);
}
