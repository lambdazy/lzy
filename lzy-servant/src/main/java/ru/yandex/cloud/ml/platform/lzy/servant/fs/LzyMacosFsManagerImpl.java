package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.commons.lang3.SystemUtils;

public class LzyMacosFsManagerImpl implements LzyFSManager {

    private final LzyFS binMount = new LzyFS();
    private final LzyFS sbinMount = new LzyFS();
    private final LzyFS baseMount = new LzyFS(Set.of("dev", "bin", "sbin"));

    @Override
    public void mount(Path mountPoint) {
        createFsDirectories(mountPoint);
        baseMount.mount(mountPoint, false, false, new String[] {"-o", "direct_io,allow_recursion"});
        binMount.mount(Path.of(mountPoint + "/bin"), false, false, new String[] {"-o", "allow_recursion"});
        sbinMount.mount(Path.of(mountPoint + "/sbin"), false, false, new String[] {"-o", "allow_recursion"});
    }

    @Override
    public void umount() {
        baseMount.umount();
        binMount.umount();
        sbinMount.umount();
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
