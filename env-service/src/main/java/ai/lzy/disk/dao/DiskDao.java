package ai.lzy.disk.dao;

import javax.annotation.Nullable;
import ai.lzy.disk.Disk;

public interface DiskDao {

    void insert(Disk disk);

    @Nullable
    Disk find(String diskId);

    void delete(String diskId);

}
