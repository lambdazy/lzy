package ai.lzy.kharon.env.model.grpc;

import ai.lzy.kharon.env.model.Disk;
import ai.lzy.kharon.env.model.DiskSpec;
import ai.lzy.kharon.env.model.DiskType;
import ai.lzy.kharon.env.model.LocalDirSpec;
import ai.lzy.kharon.env.model.S3StorageSpec;
import ai.lzy.v1.disk.LD;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcConverter {

    private static final Logger LOG = LogManager.getLogger(GrpcConverter.class);

    public static Disk from(LD.Disk disk) {
        return new Disk(disk.getId(), from(disk.getSpec()));
    }

    public static DiskSpec from(LD.DiskSpec diskSpec) {
        return switch (diskSpec.getSpecCase()) {
            case LOCAL_DIR_SPEC -> new LocalDirSpec(
                diskSpec.getLocalDirSpec().getSizeGb(),
                diskSpec.getLocalDirSpec().getFullPath(),
                diskSpec.getLocalDirSpec().getFolderName()
            );
            case S3_STORAGE_SPEC -> new S3StorageSpec(
                diskSpec.getS3StorageSpec().getSizeGb(),
                diskSpec.getS3StorageSpec().getBucket()
            );
            default -> throw new RuntimeException("invalid disk spec case");
        };
    }

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
