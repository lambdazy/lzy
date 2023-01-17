package ai.lzy.fs.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

public class LzyLinuxFsManagerImpl implements LzyFSManager {

    private final LzyFS baseMount = new LzyFS(Set.of("dev", "bin", "sbin"));

    @Override
    public void mount(Path mountPoint) {
        createFsDirectories(mountPoint);
        baseMount.mount(mountPoint, false, false,
            new String[] {"-o", "direct_io", "-o", "allow_root"} // Need user_allow_other flag in fuse.conf
        );
    }

    @Override
    public void umount() {
        baseMount.umount();
    }

    @Override
    public boolean addScript(LzyScript exec, boolean isSystem) {
        Path path;
        if (isSystem) {
            path = Path.of("/sbin").resolve(exec.location());
        } else {
            path = Path.of("/bin").resolve(exec.location());
        }
        return baseMount.addScript(exec, path);
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
