package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import org.apache.commons.lang3.SystemUtils;

public class LzyFsManagerImpl implements LzyFSManager {
    private final LzyFS binMount;
    private final LzyFS sbinMount;
    private final LzyFS baseMount = new LzyFS(Set.of("dev", "bin", "sbin"));

    public LzyFsManagerImpl() {
        if (SystemUtils.IS_OS_MAC) {
            binMount = new LzyFS();
            sbinMount = new LzyFS();
        } else {
            binMount = baseMount;
            sbinMount = baseMount;
        }
    }

    @Override
    public void mount(Path mountPoint) {
        createFsDirectories(mountPoint);
        if (SystemUtils.IS_OS_MAC) {
            baseMount.mount(mountPoint, false, false,
                new String[]{"-o", "direct_io,allow_recursion"});
            binMount.mount(Path.of(mountPoint + "/bin"), false, false,
                new String[]{"-o", "allow_recursion"});
            sbinMount.mount(Path.of(mountPoint + "/sbin"), false, false,
                new String[]{"-o", "allow_recursion"});
        } else {
            baseMount.mount(mountPoint, false, false, new String[]{"-o", "direct_io"});
        }
    }

    @Override
    public void umount() {
        if (SystemUtils.IS_OS_MAC) {
            binMount.umount();
            sbinMount.umount();
            baseMount.umount();
        } else {
            baseMount.umount();
        }
    }

    @Override
    public void addScript(LzyScript exec, boolean isSystem) {
        if (SystemUtils.IS_OS_MAC) {
            final Path path = Path.of("/").resolve(exec.location());
            if (isSystem) {
                sbinMount.addScript(exec, path);
            } else {
                binMount.addScript(exec, path);
            }
        } else {
            final Path path = Paths.get(isSystem ? "/sbin" : "/bin").resolve(exec.location());
            baseMount.addScript(exec, path);
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
