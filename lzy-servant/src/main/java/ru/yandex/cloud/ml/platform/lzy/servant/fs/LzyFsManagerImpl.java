package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.commons.lang3.SystemUtils;

public class LzyFsManagerImpl implements LzyFSManager {

    private final LzyFS binMount = new LzyFS();
    private final LzyFS sbinMount = new LzyFS();
    private final LzyFS baseMount = new LzyFS(Set.of("dev", "bin", "sbin"));

    @Override
    public void mount(Path mountPoint) {
        createFsDirectories(mountPoint);
        baseMount.mount(mountPoint, false, false,
            SystemUtils.IS_OS_MAC ? new String[]{"-o", "direct_io,allow_recursion"}
                : new String[]{"-o", "direct_io"});
        binMount.mount(Path.of(mountPoint + "/bin"), false, false,
            SystemUtils.IS_OS_MAC ? new String[]{"-o", "allow_recursion"} : new String[]{});
        sbinMount.mount(Path.of(mountPoint + "/sbin"), false, false,
            SystemUtils.IS_OS_MAC ? new String[]{"-o", "allow_recursion"} : new String[]{});
    }

    @Override
    public void umount() {
        binMount.umount();
        sbinMount.umount();
        baseMount.umount();
    }

    @Override
    public void addScript(LzyScript exec, boolean isSystem) {
        final Path path = Path.of("/").resolve(exec.location());
        if (isSystem) {
            sbinMount.addScript(exec, path);
        } else {
            binMount.addScript(exec, path);
        }
    }

    @Override
    public void addSlot(LzyFileSlot slot) {
        baseMount.addSlot(slot);
    }

    @Override
    public void removeSlot(String name) {
        baseMount.removeSlot(name);
    }

    private void createFsDirectories(Path mount) {
        try {
            Files.createDirectories(mount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
