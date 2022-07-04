package ai.lzy.disk;

public record LocalDirSpec(int sizeGb, String fullPath, String folderName) implements DiskSpec {

    @Override
    public DiskType type() {
        return DiskType.LOCAL_DIR;
    }

}
