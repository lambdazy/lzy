package ai.lzy.disk;

public record Disk(String id, DiskSpec spec) {

    public DiskType type() {
        return spec.type();
    }

}
