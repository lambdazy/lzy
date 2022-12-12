package ai.lzy.disk.model;

public record S3StorageSpec(int sizeGb, String bucket) implements DiskSpec {

    @Override
    public DiskType type() {
        return DiskType.S3_STORAGE;
    }

}
