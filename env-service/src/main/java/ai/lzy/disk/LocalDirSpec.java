package ai.lzy.disk;

public record LocalDirSpec(String fullPath, String folderName) implements DiskSpec {

    @Override
    public DiskType type() {
        return DiskType.LOCAL_DIR;
    }

}
