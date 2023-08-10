package ai.lzy.allocator.admin.dao;

import ai.lzy.allocator.model.ActiveImages;

import java.sql.SQLException;
import java.util.List;

public interface AdminDao {
    void setSyncImage(ActiveImages.Image sync) throws SQLException;

    void setImages(List<ActiveImages.PoolConfig> images) throws SQLException;

    ActiveImages.Configuration getImages() throws SQLException;
}
