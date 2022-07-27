package ai.lzy.model.disk;

import ai.lzy.v1.disk.LD;
import java.util.List;
import javax.annotation.Nullable;

public interface DiskClient {

    LD.Disk createDisk(
        String userId,
        String label,
        LD.DiskType type,
        @Nullable Integer sizeGb
    );

    LD.Disk getDisk(String userId, String diskId);

    LD.Disk deleteDisk(String userId, String diskId);

    List<LD.Disk> listUserDisks(String userId);

}
