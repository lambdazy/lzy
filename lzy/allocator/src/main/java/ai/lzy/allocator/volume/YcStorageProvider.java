package ai.lzy.allocator.volume;

import ai.lzy.allocator.model.DiskVolumeDescription;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;

@Singleton
public class YcStorageProvider implements StorageProvider {
    private static final String EMPTY_STORAGE_CLASS_NAME = "";
    public static final String YCLOUD_DISK_DRIVER = "disk-csi-driver.mks.ycloud.io";
    public static final String NFS_DRIVER = "nfs.csi.k8s.io";
    private static final String NFS_STORAGE_CLASS_NAME = "nfs-csi";

    @Override
    public String diskDriverName() {
        return YCLOUD_DISK_DRIVER;
    }

    @Override
    public String nfsDriverName() {
        return NFS_DRIVER;
    }

    @Override
    public String nfsStorageClass() {
        return NFS_STORAGE_CLASS_NAME;
    }

    @Override
    public String resolveDiskStorageClass(@Nullable DiskVolumeDescription.StorageClass storageClass) {
        if (storageClass == null) {
            return EMPTY_STORAGE_CLASS_NAME;
        }
        return switch (storageClass) {
            case HDD -> "yc-network-hdd";
            case SSD -> "yc-network-ssd";
        };
    }

    @Override
    public String resolveDiskFsType(@Nullable DiskVolumeDescription.FsType fsType) {
        if (fsType == null) {
            return "ext4";
        }
        return switch (fsType) {
            case EXT4 -> "ext4";
            case BTRFS -> "btrfs";
        };
    }
}
