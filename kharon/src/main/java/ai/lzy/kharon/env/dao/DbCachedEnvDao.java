package ai.lzy.kharon.env.dao;

import ai.lzy.kharon.env.CachedEnvStatus;
import ai.lzy.model.db.Storage;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import java.io.IOException;
import java.sql.Connection;
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

    private final Storage storage;
    private final ObjectMapper objectMapper;

    @Inject
    public DbCachedEnvDao(Storage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void insertEnv(CachedEnvInfo cachedEnv) {
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement("""
                INSERT INTO cached_envs(
                    env_id,
                    user_id,
                    workflow_name,
                    disk_id,
                    created_at,
                    status,
                    docker_image,
                    conda_yaml,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?::disk_status_type, ?, ?, ?);
                """)
        ) {
            final Instant createdAt = Instant.now();
            int index = 0;
            st.setString(++index, cachedEnv.envId());
            st.setString(++index, cachedEnv.userId());
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
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement("""
                UPDATE cached_envs
                SET status = ?, updated_at = ?
                WHERE env_id = ?
                """)
        ) {
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
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement("""
                SELECT
                    env_id,
                    user_id,
                    workflow_name,
                    disk_id,
                    status,
                    docker_image,
                    conda_yaml
                FROM cached_envs WHERE env_id = ?
                """)
        ) {
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
    public CachedEnvInfo findEnv(String userId, String workflowName, String diskId) {
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement("""
                SELECT
                    env_id,
                    user_id,
                    workflow_name,
                    disk_id,
                    status,
                    docker_image,
                    conda_yaml
                FROM cached_envs WHERE user_id = ? AND workflow_name = ? AND disk_id = ?
                """)
        ) {
            int index = 0;
            st.setString(++index, userId);
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
    public Stream<CachedEnvInfo> listEnvs(String userId, String workflowName) {
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement("""
                SELECT
                    env_id,
                    user_id,
                    workflow_name,
                    disk_id,
                    status,
                    docker_image,
                    conda_yaml
                FROM cached_envs WHERE user_id = ? AND workflow_name = ?
                """);
        ) {
            int index = 0;
            st.setString(++index, userId);
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
    public void deleteEnv(String envId) {
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement(
                "DELETE FROM cached_envs WHERE env_id = ?"
            )
        ) {
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
    public void deleteWorkflowEnvs(String userId, String workflowName) {
        try (
            final Connection connection = storage.connect();
            final PreparedStatement st = connection.prepareStatement(
                "DELETE FROM cached_envs WHERE user_id = ? AND workflow_name = ?"
            );
        ) {
            int index = 0;
            st.setString(++index, userId);
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
            rs.getString("user_id"),
            rs.getString("workflow_name"),
            rs.getString("disk_id"),
            CachedEnvStatus.valueOf(rs.getString("status")),
            rs.getString("docker_image"),
            rs.getString("conda_yaml")
        );
    }
}
