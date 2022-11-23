package ai.lzy.iam.storage.db;

import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.utils.UserVerificationType;
import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.exceptions.DaoException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
@Requires(beans = IamDataSource.class)
public class InternalUserInserter {
    public static final Logger LOG = LogManager.getLogger(InternalUserInserter.class);

    @Inject
    private IamDataSource storage;

    public void addOrUpdateInternalUser(InternalUserConfig config) {
        if (config.userName() == null) {
            LOG.info("Empty InternalUserConfig, nothing to update");
            return;
        }

        try {
            LOG.info("Insert Internal user::{} with keyType::{}", config.userName(), config.credentialType());
            Transaction.execute(storage, connection -> {
                var st = connection.prepareStatement("""
                    INSERT INTO users (user_id, auth_provider, provider_user_id, access_type, user_type, request_hash)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING""");
                st.setString(1, config.userName());
                st.setString(2, AuthProvider.INTERNAL.name());
                st.setString(3, config.userName());
                st.setString(4, UserVerificationType.ACCESS_ALLOWED.toString());
                st.setString(5, SubjectType.USER.name());
                st.setString(6, "internal-user-hash");
                st.executeUpdate();

                // H2 doesn't support `INSERT ... ON CONFLICT DO UPDATE ...`,
                // Postgres doesn't support (until PostgreSQL 15) `MERGE`,
                // so do it manually...
                st = connection.prepareStatement("""
                    SELECT name, value, user_id, type
                    FROM credentials
                    WHERE name = ? AND user_id = ?
                    FOR UPDATE""");
                st.setString(1, config.credentialName());
                st.setString(2, config.userName());
                var rs = st.executeQuery();
                if (rs.next()) {
                    st = connection.prepareStatement("""
                        UPDATE credentials
                        SET value = ?, type = ?
                        WHERE name = ? AND user_id = ?""");
                    st.setString(1, config.credentialValue());
                    st.setString(2, config.credentialType());
                    st.setString(3, config.credentialName());
                    st.setString(4, config.userName());
                    st.executeUpdate();
                } else {
                    st = connection.prepareStatement("""                
                        INSERT INTO credentials (name, value, user_id, type)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT DO NOTHING""");
                    st.setString(1, config.credentialName());
                    st.setString(2, config.credentialValue());
                    st.setString(3, config.userName());
                    st.setString(4, config.credentialType());
                    st.executeUpdate();
                }

                st = connection.prepareStatement("""
                    INSERT INTO user_resource_roles (user_id, resource_id, resource_type, role)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT DO NOTHING""");
                st.setString(1, config.userName());
                st.setString(2, Root.INSTANCE.resourceId());
                st.setString(3, Root.INSTANCE.type());
                st.setString(4, Role.LZY_INTERNAL_USER.value());
                st.executeUpdate();

                return true;
            });
        } catch (DaoException e) {
            throw new RuntimeException(e);
        }
    }
}
