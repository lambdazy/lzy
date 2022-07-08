package ai.lzy.iam.storage.db;

import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.storage.Storage;
import ai.lzy.iam.utils.UserVerificationType;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Singleton
@Requires(beans = Storage.class)
public class InternalUserInserter {

    @Inject
    private Storage storage;

    public void addOrUpdateInternalUser(InternalUserConfig config) {
        try (final Connection connection = storage.connect()) {
            PreparedStatement st = connection.prepareStatement(
                    """
                            INSERT INTO users (user_id, auth_provider, provider_user_id, access_type)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT DO NOTHING;

                            INSERT INTO credentials (name, "value", user_id, type)
                            VALUES (?, ?, ?, ?) ON CONFLICT (name, user_id) DO UPDATE SET
                            name = ?,
                            "value" = ?,
                            user_id = ?,
                            type = ?
                            ;

                            INSERT INTO user_resource_roles (user_id, resource_id, resource_type, role)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT DO NOTHING;
                            """
            );
            int parameterIndex = 0;
            st.setString(++parameterIndex, config.userName());
            st.setString(++parameterIndex, "INTERNAL_AGENT");
            st.setString(++parameterIndex, config.userName());
            st.setString(++parameterIndex, UserVerificationType.ACCESS_ALLOWED.toString());

            for (int i = 0; i < 2; i++) {
                st.setString(++parameterIndex, config.credentialName());
                st.setString(++parameterIndex, config.credentialValue());
                st.setString(++parameterIndex, config.userName());
                st.setString(++parameterIndex, config.credentialType());
            }

            Root root = new Root();
            st.setString(++parameterIndex, config.userName());
            st.setString(++parameterIndex, root.resourceId());
            st.setString(++parameterIndex, root.type());
            st.setString(++parameterIndex, Role.LZY_INTERNAL_USER.role());
            st.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
