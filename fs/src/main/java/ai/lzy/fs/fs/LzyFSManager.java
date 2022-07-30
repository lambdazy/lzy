package ai.lzy.fs.fs;

import java.nio.file.Path;
import java.util.Set;

public interface LzyFSManager {
    static Set<String> roots() {
        return Set.of("sbin", "bin", "dev");
    }

    void mount(Path mountPoint);

    void umount();

    boolean addScript(LzyScript exec, boolean isSystem);

    void addSlot(LzyFileSlot slot);

    void removeSlot(String name);
}
