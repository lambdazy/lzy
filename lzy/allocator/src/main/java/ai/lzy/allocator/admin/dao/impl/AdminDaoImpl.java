package ai.lzy.allocator.admin.dao.impl;

import ai.lzy.allocator.admin.dao.AdminDao;
import ai.lzy.allocator.model.ActiveImages;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class AdminDaoImpl implements AdminDao {
    private static final Logger LOG = LogManager.getLogger(AdminDaoImpl.class);

    private final AllocatorDataSource storage;

    public AdminDaoImpl(AllocatorDataSource storage) {
        this.storage = storage;
    }

    @Override
    public ActiveImages.Configuration getImages() throws SQLException {
        return DbOperation.execute(null, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(
                "SELECT kind, image, additional_images FROM images"))
            {
                var rs = st.executeQuery();

                String sync = null;
                List<ActiveImages.WorkerImage> workers = new ArrayList<>();
                List<ActiveImages.JupyterLabImage> jupyterLabs = new ArrayList<>();

                while (rs.next()) {
                    var kind = rs.getString("kind");
                    var image = rs.getString("image");
                    var additionalImagesArr = rs.getArray("additional_images");

                    switch (kind) {
                        case "WORKER" -> workers.add(ActiveImages.WorkerImage.of(image));
                        case "SYNC" -> {
                            if (sync != null) {
                                LOG.error("Duplicated sync image: {} and {}", sync, image);
                                throw new RuntimeException("Duplicated sync image: %s and %s".formatted(sync, image));
                            }
                            sync = image;
                        }
                        case "JUPYTERLAB" -> jupyterLabs.add(
                            ActiveImages.JupyterLabImage.of(image, (String[]) additionalImagesArr.getArray()));
                    }
                }

                return new ActiveImages.Configuration(
                    ActiveImages.SyncImage.of(sync),
                    workers,
                    jupyterLabs);
            }
        });
    }

    @Override
    public void setWorkerImages(List<ActiveImages.WorkerImage> workers) throws SQLException {
        try (var tx = TransactionHandle.create(storage);
             var conn = tx.connect();
             var drop = conn.prepareStatement(
                 "DELETE FROM images WHERE kind = 'WORKER'::image_kind");
             var insert = conn.prepareStatement("" +
                 "INSERT INTO images (kind, image, additional_images) VALUES ('WORKER'::image_kind, ?, NULL)"))
        {
            drop.execute();

            for (var worker : workers) {
                insert.setString(1, worker.image());
                insert.addBatch();
            }

            insert.executeBatch();

            tx.commit();
        }
    }

    @Override
    public void setSyncImage(ActiveImages.SyncImage sync) throws SQLException {
        DbOperation.execute(null, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement("""
                INSERT INTO images (kind, image, additional_images) VALUES ('SYNC'::image_kind, ?, NULL)
                ON CONFLICT (kind) WHERE kind = 'SYNC'::image_kind DO UPDATE SET image = ?
                """))
            {
                st.setString(1, sync.image());
                st.setString(2, sync.image());
                st.executeUpdate();
            }
        });
    }

    @Override
    public void setJupyterLabImages(List<ActiveImages.JupyterLabImage> jupyterLabs) throws SQLException {
        try (var tx = TransactionHandle.create(storage);
             var conn = tx.connect();
             var drop = conn.prepareStatement(
                 "DELETE FROM images WHERE kind = 'JUPYTERLAB'::image_kind");
             var insert = conn.prepareStatement("" +
                 "INSERT INTO images (kind, image, additional_images) VALUES ('JUPYTERLAB'::image_kind, ?, ?)"))
        {
            drop.execute();

            for (var jl : jupyterLabs) {
                insert.setString(1, jl.mainImage());
                insert.setArray(2, conn.createArrayOf("TEXT", jl.additionalImages()));
                insert.addBatch();
            }

            insert.executeBatch();

            tx.commit();
        }
    }
}
