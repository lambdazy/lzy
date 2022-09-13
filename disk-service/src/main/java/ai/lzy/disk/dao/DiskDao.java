package ai.lzy.disk.dao;

import javax.annotation.Nullable;
import ai.lzy.disk.model.Disk;

public interface DiskDao {

    void insert(String userId, Disk disk);

    @Nullable
    Disk find(String userId, String diskId);

    void delete(String userId, String diskId);

}
