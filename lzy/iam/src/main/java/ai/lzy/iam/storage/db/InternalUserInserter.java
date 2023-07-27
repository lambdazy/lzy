package ai.lzy.iam.storage.db;

import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.utils.UserVerificationType;
import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
                try (var upsertUserSt = connection.prepareStatement("""
                    INSERT INTO users (user_id, auth_provider, provider_user_id, access_type, user_type, request_hash)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING""");
                     var selectSt = connection.prepareStatement("""
                         SELECT name, value, user_id, type
                         FROM credentials
                         WHERE name = ? AND user_id = ?
                         FOR UPDATE""");
                     var upsertRoleSt = connection.prepareStatement("""
                         INSERT INTO user_resource_roles (user_id, resource_id, resource_type, role)
                         VALUES (?, ?, ?, ?)
                         ON CONFLICT DO NOTHING"""))
                {
                    upsertUserSt.setString(1, config.userName());
                    upsertUserSt.setString(2, AuthProvider.INTERNAL.name());
                    upsertUserSt.setString(3, config.userName());
                    upsertUserSt.setString(4, UserVerificationType.ACCESS_ALLOWED.toString());
                    upsertUserSt.setString(5, SubjectType.USER.name());
                    upsertUserSt.setString(6, "internal-user-hash");
                    upsertUserSt.executeUpdate();

                    // H2 doesn't support `INSERT ... ON CONFLICT DO UPDATE ...`,
                    // Postgres doesn't support (until PostgreSQL 15) `MERGE`,
                    // so do it manually...
                    ;
                    selectSt.setString(1, config.credentialName());
                    selectSt.setString(2, config.userName());
                    var rs = selectSt.executeQuery();

                    if (rs.next()) {
                        try (var updateSt = connection.prepareStatement("""
                            UPDATE credentials
                            SET value = ?, type = ?
                            WHERE name = ? AND user_id = ?"""))
                        {
                            updateSt.setString(1, config.credentialValue());
                            updateSt.setString(2, config.credentialType());
                            updateSt.setString(3, config.credentialName());
                            updateSt.setString(4, config.userName());
                            updateSt.executeUpdate();
                        }
                    } else {
                        try (var insertCredsSt = connection.prepareStatement("""                
                            INSERT INTO credentials (name, value, user_id, type)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT DO NOTHING"""))
                        {
                            insertCredsSt.setString(1, config.credentialName());
                            insertCredsSt.setString(2, config.credentialValue());
                            insertCredsSt.setString(3, config.userName());
                            insertCredsSt.setString(4, config.credentialType());
                            insertCredsSt.executeUpdate();
                        }
                    }

                    upsertRoleSt.setString(1, config.userName());
                    upsertRoleSt.setString(2, Root.INSTANCE.resourceId());
                    upsertRoleSt.setString(3, Root.INSTANCE.type());
                    upsertRoleSt.setString(4, Role.LZY_INTERNAL_USER.value());
                    upsertRoleSt.executeUpdate();

                    return true;
                }
            });
        } catch (DaoException e) {
            throw new RuntimeException(e);
        }
    }

    public Subject addAdminUser(String name, String publicKey) throws SQLException {
        try (var tx = TransactionHandle.create(storage);
             var conn = tx.connect();
             PreparedStatement addUserSt = conn.prepareStatement("""
                INSERT INTO users
                    (user_id, auth_provider, provider_user_id, access_type, user_type, request_hash)
                VALUES (?, ?, ?, ?, ?, ?)""");
             PreparedStatement addCredsSt = conn.prepareStatement("""
                INSERT INTO credentials (name, value, user_id, type)
                VALUES (?, ?, ?, ?)""");
             PreparedStatement addRoleSt = conn.prepareStatement("""
                INSERT INTO user_resource_roles (user_id, resource_id, resource_type, role)
                VALUES (?, ?, ?, ?)"""))
        {
            addUserSt.setString(1, name);
            addUserSt.setString(2, AuthProvider.INTERNAL.name());
            addUserSt.setString(3, name);
            addUserSt.setString(4, UserVerificationType.ACCESS_ALLOWED.toString());
            addUserSt.setString(5, SubjectType.USER.name());
            addUserSt.setString(6, name + "_hash");
            addUserSt.executeUpdate();

            addCredsSt.setString(1, "main");
            addCredsSt.setString(2, publicKey);
            addCredsSt.setString(3, name);
            addCredsSt.setString(4, CredentialsType.PUBLIC_KEY.name());
            addCredsSt.executeUpdate();

            addRoleSt.setString(1, name);
            addRoleSt.setString(2, Root.INSTANCE.resourceId());
            addRoleSt.setString(3, Root.INSTANCE.type());
            addRoleSt.setString(4, Role.LZY_INTERNAL_ADMIN.value());
            addRoleSt.executeUpdate();

            tx.commit();
        }

        return new User(name, AuthProvider.INTERNAL, name);
    }
}
