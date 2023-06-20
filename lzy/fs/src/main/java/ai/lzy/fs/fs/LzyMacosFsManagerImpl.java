package ai.lzy.fs.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

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
    public boolean addScript(LzyScript exec, boolean isSystem) {
        final Path path = Path.of("/").resolve(exec.location());
        if (isSystem) {
            return sbinMount.addScript(exec, path);
        } else {
            return binMount.addScript(exec, path);
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
