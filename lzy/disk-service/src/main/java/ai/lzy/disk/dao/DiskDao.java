package ai.lzy.disk.dao;

import ai.lzy.disk.model.Disk;

import jakarta.annotation.Nullable;
import javax.annotation.Nullable;

public interface DiskDao {

    void insert(String userId, Disk disk);

    @Nullable
    Disk find(String userId, String diskId);

    void delete(String userId, String diskId);

}
