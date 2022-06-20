package ai.lzy.env.dao;

import ai.lzy.common.db.DbConnector;
import ai.lzy.disk.DiskType;
import ai.lzy.env.CachedEnvStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DbCachedEnvDao implements CachedEnvDao {

    private static final Logger LOG = LogManager.getLogger(DbCachedEnvDao.class);

    private final DbConnector connector;
    private final ObjectMapper objectMapper;

    @Inject
    public DbCachedEnvDao(DbConnector connector, ObjectMapper objectMapper) {
        this.connector = connector;
        this.objectMapper = objectMapper;
    }

    @Override
    public void insertEnv(CachedEnvInfo cachedEnv) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement("""
                INSERT INTO cached_envs(
                    env_id,
                    workflow_name,
                    disk_id,
                    created_at,
                    status,
                    docker_image,
                    yaml_config,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?::disk_status_type, ?, ?, ?);
                """);
            final Instant createdAt = Instant.now();
            int index = 0;
            st.setString(++index, cachedEnv.envId());
            st.setString(++index, cachedEnv.workflowName());
            st.setString(++index, cachedEnv.diskId());
            st.setTimestamp(++index, Timestamp.from(createdAt));
            st.setString(++index, cachedEnv.status().name());
            st.setString(++index, cachedEnv.dockerImage());
            st.setString(++index, cachedEnv.yamlConfig());
            st.setTimestamp(++index, Timestamp.from(createdAt));
            st.executeUpdate();
        } catch (SQLException e) {
            String errorMessage = String.format("Failed to insert cached env (envId=%s)", cachedEnv.envId());
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Override
    public CachedEnvInfo setEnvStatus(String envId, CachedEnvStatus status) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement("""
               UPDATE cached_envs
               SET status = ?, updated_at = ?
               WHERE env_id = ?
               """);
            final Instant updatedAt = Instant.now();
            int index = 0;
            st.setString(++index, status.name());
            st.setTimestamp(++index, Timestamp.from(updatedAt));
            st.setString(++index, envId);
            if (st.executeUpdate() == 0) {
                String errorMessage = String.format("Cached env (envId=%s) not found", envId);
                LOG.error(errorMessage);
                throw new RuntimeException(errorMessage);
            }
        } catch (SQLException e) {
            String errorMessage = String.format("Failed to insert cached env (envId=%s)", envId);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }

        CachedEnvInfo updatedEnv = findEnv(envId);
        if (updatedEnv == null) {
            String errorMessage = String.format("Cached env (envId=%s) not found after updating status", envId);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        return updatedEnv;
    }

    @Nullable
    @Override
    public CachedEnvInfo findEnv(String envId) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement("""
                SELECT 
                    env_id,
                    workflow_name,
                    disk_id,
                    status,
                    docker_image,
                    yaml_config
                FROM cached_envs WHERE env_id = ?
                """);
            int index = 0;
            st.setString(++index, envId);
            ResultSet rs = st.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return parseCachedEnv(rs);
        } catch (IOException | SQLException e) {
            String errorMessage = String.format("Failed to find cached env (envId=%s)", envId);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Nullable
    @Override
    public CachedEnvInfo findEnv(String workflowName, String diskId) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement("""
                SELECT
                    env_id,
                    workflow_name,
                    disk_id,
                    status,
                    docker_image,
                    yaml_config
                FROM cached_envs WHERE workflow_name = ? AND disk_id = ?
                """);
            int index = 0;
            st.setString(++index, workflowName);
            st.setString(++index, diskId);
            ResultSet rs = st.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return parseCachedEnv(rs);
        } catch (IOException | SQLException e) {
            String errorMessage = String.format(
                "Failed to find cached env (workflowName=%s, diskId=%s)",
                workflowName, diskId
            );
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Override
    public Stream<CachedEnvInfo> listEnvs(String workflowName) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement("""
                SELECT
                    env_id,
                    workflow_name,
                    disk_id,
                    status,
                    docker_image,
                    yaml_config
                FROM cached_envs WHERE workflow_name = ?
                """);
            int index = 0;
            st.setString(++index, workflowName);
            ResultSet rs = st.executeQuery();
            List<CachedEnvInfo> cachedEnvs = new ArrayList<>();
            while (rs.next()) {
                cachedEnvs.add(parseCachedEnv(rs));
            }
            return cachedEnvs.stream();
        } catch (IOException | SQLException e) {
            String errorMessage = String.format("Failed to get list of cached env (workflowName=%s)", workflowName);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Override
    public Stream<CachedEnvInfo> listEnvs(String workflowName, DiskType diskType) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement("""
                SELECT 
                    env_id,
                    workflow_name,
                    cached_envs.disk_id,
                    status,
                    docker_image,
                    yaml_config
                FROM cached_envs INNER JOIN disks ON cached_envs.disk_id = disks.disk_id
                WHERE workflow_name = ? AND disk_provider = ?::disk_provider_type
                """);
            int index = 0;
            st.setString(++index, workflowName);
            st.setString(++index, diskType.name());
            ResultSet rs = st.executeQuery();
            List<CachedEnvInfo> cachedEnvs = new ArrayList<>();
            while (rs.next()) {
                cachedEnvs.add(parseCachedEnv(rs));
            }
            return cachedEnvs.stream();
        } catch (IOException | SQLException e) {
            String errorMessage = String.format(
                "Failed to get list of cached env (workflowName=%s, diskType=%s)",
                workflowName, diskType
            );
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Override
    public void deleteEnv(String envId) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement(
                "DELETE FROM cached_envs WHERE env_id = ?"
            );
            int index = 0;
            st.setString(++index, envId);
            st.execute();
        } catch (SQLException e) {
            String errorMessage = String.format("Failed to delete cached env (envId=%s)", envId);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Override
    public void deleteWorkflowEnvs(String workflowName) {
        try {
            final PreparedStatement st = connector.connect().prepareStatement(
                "DELETE FROM cached_envs WHERE workflow_name = ?"
            );
            int index = 0;
            st.setString(++index, workflowName);
            st.execute();
        } catch (SQLException e) {
            String errorMessage = String.format("Failed to delete cached env (workflowName=%s)", workflowName);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    private CachedEnvInfo parseCachedEnv(ResultSet rs) throws SQLException, IOException  {
        return new CachedEnvInfo(
            rs.getString("env_id"),
            rs.getString("workflow_name"),
            rs.getString("disk_id"),
            CachedEnvStatus.valueOf(rs.getString("status")),
            rs.getString("docker_image"),
            rs.getString("yaml_config")
        );
    }
}
