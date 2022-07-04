package ai.lzy.disk.common;

import ai.lzy.disk.Disk;
import ai.lzy.disk.DiskSpec;
import ai.lzy.disk.DiskType;
import ai.lzy.disk.LocalDirSpec;
import ai.lzy.disk.S3StorageSpec;
import ai.lzy.v1.disk.LD;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcConverter {

    private static final Logger LOG = LogManager.getLogger(GrpcConverter.class);

    public static DiskType from(LD.DiskType diskType) {
        return switch (diskType) {
            case UNSPECIFIED -> {
                DiskType replacedType = DiskType.S3_STORAGE;
                LOG.warn("Received {} disk type, converted to {}", diskType, replacedType);
                yield replacedType;
            }
            default -> DiskType.valueOf(diskType.name());
        };
    }

    public static LD.Disk to(Disk disk) {
        return LD.Disk.newBuilder()
            .setId(disk.id())
            .setSpec(to(disk.spec()))
            .build();
    }

    public static LD.DiskSpec to(DiskSpec diskSpec) {
        if (diskSpec instanceof LocalDirSpec spec) {
            return LD.DiskSpec.newBuilder().setLocalDirSpec(
                LD.LocalDirSpec.newBuilder()
                    .setSizeGb(spec.sizeGb())
                    .setFullPath(spec.fullPath())
                    .setFolderName(spec.folderName())
                    .build()
                )
                .build();
        }
        if (diskSpec instanceof S3StorageSpec spec) {
            return LD.DiskSpec.newBuilder().setS3StorageSpec(
                LD.S3StorageSpec.newBuilder()
                    .setSizeGb(spec.sizeGb())
                    .setBucket(spec.bucket())
                    .build()
                )
                .build();
        }
        LOG.warn("Received unexpected {} disk type", diskSpec.getClass().toString());
        return LD.DiskSpec.getDefaultInstance();
    }

    public static LD.DiskType to(DiskType diskType) {
        return LD.DiskType.valueOf(diskType.name());
    }


}
