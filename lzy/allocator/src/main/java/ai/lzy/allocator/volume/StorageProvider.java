package ai.lzy.allocator.volume;

import ai.lzy.allocator.model.DiskVolumeDescription;
import jakarta.annotation.Nullable;

public interface StorageProvider {
    String diskDriverName();

    String nfsDriverName();

    String nfsStorageClass();

    String resolveDiskStorageClass(@Nullable DiskVolumeDescription.StorageClass storageClass);

    String resolveDiskFsType(@Nullable DiskVolumeDescription.FsType fsType);
}
