package ai.lzy.allocator.disk;

public enum DiskType {
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
            case 1 -> HDD;
            case 2 -> SSD;
            case 3 -> NR_SSD;
            default -> throw new IllegalArgumentException("Unknown disk type id " + value);
        };
    }

    public String toYcName() {
        return switch (this) {
            case HDD -> "network-hdd";
            case SSD -> "network-ssd";
            case NR_SSD -> "network-ssd-nonreplicated";
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
