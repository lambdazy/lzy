package ai.lzy.allocator.disk;

public enum DiskType {
    HDD(0),
    SSD(1),
    NR_SSD(2);

    private final int value;

    DiskType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static DiskType fromNumber(int value) {
        return switch (value) {
            case 0 -> HDD;
            case 1 -> SSD;
            case 2 -> NR_SSD;
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
