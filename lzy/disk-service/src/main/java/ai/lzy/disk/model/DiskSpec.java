package ai.lzy.disk.model;

import java.util.Map;

public sealed interface DiskSpec permits LocalDirSpec, S3StorageSpec {

    static DiskSpec fromMap(Map<String, String> spec, DiskType type) {
        return switch (type) {
            case LOCAL_DIR -> new LocalDirSpec(
                Integer.parseInt(spec.get("sizeGb")),
                spec.get("fullPath"),
                spec.get("folderName")
            );
            case S3_STORAGE -> new S3StorageSpec(
                Integer.parseInt(spec.get("sizeGb")),
                spec.get("bucket")
            );
        };
    }

    DiskType type();

}
