package ai.lzy.disk;

public record S3StorageSpec(String bucket) implements DiskSpec {

    @Override
    public DiskType type() {
        return DiskType.S3_STORAGE;
    }

}
