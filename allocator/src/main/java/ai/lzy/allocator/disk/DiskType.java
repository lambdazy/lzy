package ai.lzy.allocator.disk;

public enum DiskType {
    DISK_TYPE_UNSPECIFIED(0),
    HDD(1),
    SSD(2),
    NR_SSD(3);

    private final int value;

    DiskType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static DiskType fromNumber(int value) {
        return switch (value) {
            case 0 -> DISK_TYPE_UNSPECIFIED;
            case 1 -> HDD;
            case 2 -> SSD;
            case 3 -> NR_SSD;
            default -> throw new IllegalArgumentException("Unknown get type idx");
        };
    }

    public String toYcName() {
        return switch (this) {
            case HDD -> "network-hdd";
            case SSD -> "network-ssd";
            case NR_SSD -> "network-ssd-nonreplicated";
            default -> throw new IllegalStateException("Unknown disk type");
        };
    }

    public static DiskType fromYcName(String typeId) {
        return switch (typeId) {
            case "network-hdd" -> HDD;
            case "network-ssd" -> SSD;
            case "network-ssd-nonreplicated" -> NR_SSD;
            default -> throw new IllegalArgumentException("Unknown disk type");
        };
    }
}
