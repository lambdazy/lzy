package ai.lzy.disk.model;

public record Disk(String id, DiskSpec spec) {

    public DiskType type() {
        return spec.type();
    }

}
