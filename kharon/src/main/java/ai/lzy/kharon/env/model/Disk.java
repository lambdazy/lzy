package ai.lzy.kharon.env.model;

public record Disk(String id, DiskSpec spec) {

    public DiskType type() {
        return spec.type();
    }

}
