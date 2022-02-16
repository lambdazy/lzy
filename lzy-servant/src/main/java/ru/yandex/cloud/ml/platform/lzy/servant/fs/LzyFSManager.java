package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.nio.file.Path;
import java.util.Set;

public interface LzyFSManager {
    static Set<String> roots() {
        return Set.of("sbin", "bin", "dev");
    }

    void mount(Path mountPoint);

    void umount();

    void addScript(LzyScript exec, boolean isSystem);

    void addSlot(LzyFileSlot slot);

    void removeSlot(String name);
}
