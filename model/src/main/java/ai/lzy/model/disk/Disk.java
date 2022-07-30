package ai.lzy.model.disk;

public record Disk(String id, DiskSpec spec) {

    public DiskType type() {
        return spec.type();
    }

}
