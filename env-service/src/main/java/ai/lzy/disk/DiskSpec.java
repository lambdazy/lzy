package ai.lzy.disk;

import java.util.Map;

public sealed interface DiskSpec permits LocalDirSpec, S3StorageSpec {

    static DiskSpec fromMap(Map<String, String> spec, DiskType type) {
        return switch (type) {
            case LOCAL_DIR -> new LocalDirSpec(spec.get("path"), spec.get("folder_name"));
            case S3_STORAGE -> new S3StorageSpec(spec.get("bucket"));
        };
    }

    DiskType type();

}
