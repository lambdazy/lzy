package ai.lzy.common;

import ai.lzy.disk.Disk;
import ai.lzy.disk.DiskType;
import ai.lzy.disk.service.DiskService;
import ai.lzy.priv.v1.LED;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcConverter {

    private static final Logger LOG = LogManager.getLogger(GrpcConverter.class);

    public static DiskType from(LED.DiskType diskType) {
        return switch (diskType) {
            case UNRECOGNIZED, UNSPECIFIED -> {
                DiskType replacedType = DiskType.S3_STORAGE;
                LOG.warn("Received {} disk type, converted to {}", diskType, replacedType);
                yield replacedType;
            }
            default -> DiskType.valueOf(diskType.name());
        };
    }

    public static LED.DiskType to(DiskType diskType) {
        return LED.DiskType.valueOf(diskType.name());
    }

    public static LED.Disk to(Disk disk) {
        return LED.Disk.newBuilder()
            .setDiskId(disk.id())
            .setType(to(disk.type()))
            .build();
    }


}
