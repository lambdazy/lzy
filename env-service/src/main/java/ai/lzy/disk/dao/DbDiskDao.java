package ai.lzy.disk.dao;

import ai.lzy.common.db.DbConnector;
import ai.lzy.disk.Disk;
import ai.lzy.disk.DiskSpec;
import ai.lzy.disk.DiskType;
import ai.lzy.disk.LocalDirSpec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DbDiskDao implements DiskDao {

    private static final Logger LOG = LogManager.getLogger(DbDiskDao.class);

    private final DbConnector connector;
    private final ObjectMapper objectMapper;

    @Inject
    public DbDiskDao(DbConnector connector, ObjectMapper objectMapper) {
        this.connector = connector;
        this.objectMapper = objectMapper;
    }

    @Override
    public void insert(Disk disk) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement("""
                INSERT INTO disks(
                    disk_id,
                    disk_provider,
                    disk_spec,
                    created_at
                ) VALUES (?, ?::disk_provider_type, ?::jsonb, ?)
                """);
            int index = 0;
            st.setString(++index, disk.id());
            st.setString(++index, disk.type().toString());
            st.setString(++index, objectMapper.writeValueAsString(disk.spec()));
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
    public Disk find(String diskId) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement("""
                SELECT disk_id, disk_provider, disk_spec
                FROM disks WHERE disk_id = ?
                """);
            int index = 0;
            st.setString(++index, diskId);
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
    public void delete(String diskId) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement(
                "DELETE FROM disks WHERE disk_id = ?"
            );
            int index = 0;
            st.setString(++index, diskId);
            st.execute();
        } catch (SQLException e) {
            String errorMessage = String.format("Failed to delete disk (diskId=%s)", diskId);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    private Disk parseDisk(ResultSet rs) throws SQLException, IOException  {
        final String diskId = rs.getString("disk_id");
        final DiskSpec diskSpec = DiskSpec.fromMap(
            objectMapper.readValue(rs.getString("disk_spec"), new TypeReference<Map<String, String>>() {}),
            DiskType.valueOf(rs.getString("disk_provider"))
        );
        return new Disk(diskId, diskSpec);
    }
}
