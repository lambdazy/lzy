package ai.lzy.allocator.disk;

public record DiskSpec(
    String name,
    DiskType type,
    int sizeGb,
    String zone
) {}
