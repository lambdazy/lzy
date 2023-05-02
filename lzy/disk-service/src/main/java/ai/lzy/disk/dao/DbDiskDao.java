package ai.lzy.disk.dao;

import ai.lzy.disk.model.Disk;
import ai.lzy.disk.model.DiskSpec;
import ai.lzy.disk.model.DiskType;
import ai.lzy.model.db.Storage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import jakarta.annotation.Nullable;
import javax.annotation.Nullable;

public class DbDiskDao implements DiskDao {

    private static final Logger LOG = LogManager.getLogger(DbDiskDao.class);

    private final Storage storage;
    private final ObjectMapper objectMapper;

    @Inject
    public DbDiskDao(Storage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void insert(String userId, Disk disk) {
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement("""
                INSERT INTO disks(
                    disk_id,
                    user_id,
                    disk_provider,
                    disk_spec_json,
                    created_at
                ) VALUES (?, ?, ?::disk_provider_type, ?, ?)
                """);
        ) {
            String diskSpecJson = objectMapper.writeValueAsString(disk.spec());
            int index = 0;
            st.setString(++index, disk.id());
            st.setString(++index, userId);
            st.setString(++index, disk.type().toString());
            st.setString(++index, diskSpecJson);
            st.setTimestamp(++index, Timestamp.from(Instant.now()));
            st.executeUpdate();
        } catch (IOException | SQLException e) {
            String errorMessage = String.format("Failed to insert disk (diskId=%s)", disk.id());
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Nullable
    @Override
    public Disk find(String userId, String diskId) {
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement("""
                SELECT user_id, disk_id, disk_provider, disk_spec_json
                FROM disks WHERE disk_id = ? AND user_id = ?
                """);
            )
        {

            int index = 0;
            st.setString(++index, diskId);
            st.setString(++index, userId);
            ResultSet rs = st.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return parseDisk(rs);
        } catch (IOException | SQLException e) {
            String errorMessage = String.format("Failed to find disk (diskId=%s)", diskId);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Override
    public void delete(String userId, String diskId) {
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement(
                "DELETE FROM disks WHERE disk_id = ? AND user_id = ?"
            )
        ) {
            int index = 0;
            st.setString(++index, diskId);
            st.setString(++index, userId);
            st.execute();
        } catch (SQLException e) {
            String errorMessage = String.format("Failed to delete disk (diskId=%s)", diskId);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    private Disk parseDisk(ResultSet rs) throws SQLException, IOException  {
        final String diskId = rs.getString("disk_id");

    String specJson = rs.getString("disk_spec_json");

        final DiskSpec diskSpec = DiskSpec.fromMap(
            objectMapper.readValue(specJson, new TypeReference<>() {}),
            DiskType.valueOf(rs.getString("disk_provider"))
        );
        return new Disk(diskId, diskSpec);
    }
}
