package ai.lzy.allocator.volume;

import ai.lzy.allocator.model.DiskVolumeDescription;

public interface StorageProvider {
    String diskDriverName();

    String nfsDriverName();

    String nfsStorageClass();

    String resolveDiskStorageClass(DiskVolumeDescription.StorageClass storageClass);
}
