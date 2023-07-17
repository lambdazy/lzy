package ai.lzy.allocator.admin.dao;

import ai.lzy.allocator.model.ActiveImages;

import java.sql.SQLException;
import java.util.List;

public interface AdminDao {
    ActiveImages.Configuration getImages() throws SQLException;
    void setWorkerImages(List<ActiveImages.WorkerImage> workers) throws SQLException;
    void setSyncImage(ActiveImages.SyncImage sync) throws SQLException;
    void setJupyterLabImages(List<ActiveImages.JupyterLabImage> jupyterLabs) throws SQLException;
}
