package ai.lzy.disk.dao;

import ai.lzy.disk.common.db.DataSourceStorage;
import ai.lzy.disk.Disk;
import ai.lzy.disk.DiskSpec;
import ai.lzy.disk.DiskType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DbDiskDao implements DiskDao {

    private static final Logger LOG = LogManager.getLogger(DbDiskDao.class);

    private final DataSourceStorage connector;
    private final ObjectMapper objectMapper;

    @Inject
    public DbDiskDao(DataSourceStorage connector, ObjectMapper objectMapper) {
        this.connector = connector;
        this.objectMapper = objectMapper;
    }

    @Override
    public void insert(String userId, Disk disk) {
        try (
            final Connection connection = connector.connect();
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

            final Connection connection = connector.connect();
            final PreparedStatement st = connection.prepareStatement("""
                SELECT user_id, disk_id, disk_provider, disk_spec_json
                FROM disks WHERE disk_id = ? AND user_id = ?
                """);
            ){

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
        try {
            final PreparedStatement st = connector.connect().prepareStatement(
                "DELETE FROM disks WHERE disk_id = ? AND user_id = ?"
            );
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
