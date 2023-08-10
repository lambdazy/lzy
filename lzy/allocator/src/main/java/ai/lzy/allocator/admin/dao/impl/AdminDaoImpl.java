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
import java.util.Arrays;
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
                "SELECT kind, images, sync_image, additional_images, pool_kind, pool_name FROM images"))
            {
                var rs = st.executeQuery();

                String sync = null;
                List<ActiveImages.PoolConfig> workers = new ArrayList<>();

                while (rs.next()) {
                    var kind = rs.getString("kind");
                    var syncImg = rs.getString("sync_image");

                    switch (kind) {
                        case "SYNC" -> {
                            if (sync != null) {
                                LOG.error("Duplicated sync image: {} and {}", sync, syncImg);
                                throw new RuntimeException("Duplicated sync image: %s and %s".formatted(sync, syncImg));
                            }
                            sync = syncImg;
                        }
                        case "CACHE" -> {
                            var images = rs.getArray("images");
                            var additionalImagesArr = rs.getArray("additional_images");
                            var poolKind = rs.getString("pool_kind");
                            var poolName = rs.getString("pool_name");
                            ActiveImages.DindImages dindImages = null;
                            if (syncImg != null && additionalImagesArr != null) {
                                dindImages = ActiveImages.DindImages.of(
                                    syncImg,
                                    Arrays.stream(((String[]) additionalImagesArr.getArray())).toList()
                                );
                            }
                            workers.add(
                                ActiveImages.PoolConfig.of(
                                    Arrays.stream(((String[]) images.getArray())).map(ActiveImages.Image::of).toList(),
                                    dindImages,
                                    poolKind,
                                    poolName
                            ));
                        }
                    }
                }

                return new ActiveImages.Configuration(
                    ActiveImages.Image.of(sync),
                    workers
                );
            }
        });
    }

    @Override
    public void setImages(List<ActiveImages.PoolConfig> images) throws SQLException {
        try (var tx = TransactionHandle.create(storage);
             var conn = tx.connect();
             var drop = conn.prepareStatement(
                 "DELETE FROM images WHERE kind = 'CACHE'::image_kind");
             var insert = conn.prepareStatement("""
                    INSERT INTO images (kind, images, sync_image, additional_images, pool_kind, pool_name)
                    VALUES ('CACHE'::image_kind, ?, ?, ?, ?, ?)
                    """))
        {
            drop.execute();

            for (var pool : images) {
                insert.setArray(
                    1,
                    conn.createArrayOf("TEXT", pool.images().stream().map(ActiveImages.Image::image).toArray()));
                if (pool.dindImages() != null) {
                    insert.setString(2, pool.dindImages().dindImage());
                    insert.setArray(
                        3,
                        conn.createArrayOf("TEXT", pool.dindImages().additionalImages().toArray()));
                } else {
                    insert.setString(2, null);
                    insert.setArray(3, null);
                }
                insert.setString(4, pool.kind());
                insert.setString(5, pool.name());
                insert.addBatch();
            }

            insert.executeBatch();

            tx.commit();
        }
    }

    @Override
    public void setSyncImage(ActiveImages.Image sync) throws SQLException {
        DbOperation.execute(null, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement("""
                INSERT INTO images (kind, images, sync_image, additional_images, pool_kind, pool_name)
                VALUES ('SYNC'::image_kind, NULL,  ?, NULL, '', '')
                ON CONFLICT (kind) WHERE kind = 'SYNC'::image_kind DO UPDATE SET sync_image = ?
                """))
            {
                st.setString(1, sync.image());
                st.setString(2, sync.image());
                st.executeUpdate();
            }
        });
    }

}
