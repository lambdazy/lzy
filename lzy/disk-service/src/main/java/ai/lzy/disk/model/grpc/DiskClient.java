package ai.lzy.disk.model.grpc;

import ai.lzy.disk.model.EntityNotFoundException;
import ai.lzy.v1.disk.LD;

import java.util.List;
import jakarta.annotation.Nullable;
import javax.annotation.Nullable;

public interface DiskClient {

    LD.Disk createDisk(
        String userId,
        String label,
        LD.DiskType type,
        @Nullable Integer sizeGb
    );

    LD.Disk getDisk(String userId, String diskId) throws EntityNotFoundException;

    LD.Disk deleteDisk(String userId, String diskId) throws EntityNotFoundException;

    List<LD.Disk> listUserDisks(String userId);

}
