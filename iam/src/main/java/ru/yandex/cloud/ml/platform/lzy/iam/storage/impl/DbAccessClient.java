package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthBadRequestException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthInternalException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.Role;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.DbStorage;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
@Requires(beans = DbStorage.class)
public class DbAccessClient implements AccessClient {

    @Inject
    private DbStorage storage;

    @Override
    public boolean hasResourcePermission(String userId, String resourceId, AuthPermission permission)
            throws AuthException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "SELECT user_id FROM user_resource_roles "
                        + "WHERE user_id = ? "
                        + "AND resource_id = ? "
                        + ";"
        )) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, userId);
            st.setString(++parameterIndex, resourceId);
            final ResultSet rs = st.executeQuery();
            if (!rs.next()) {
                throw new AuthBadRequestException("Resource: " + resourceId + " not found");
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "SELECT user_id FROM user_resource_roles "
                        + "WHERE user_id = ? "
                        + "AND resource_id = ? "
                        + "AND " + queryByPermission(permission)
        )) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, userId);
            st.setString(++parameterIndex, resourceId);
            final ResultSet rs = st.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
        return false;
    }

    private String queryByPermission(AuthPermission permission) {
        StringBuilder query = new StringBuilder("(");
        final boolean[] first = {true};
        Role.rolesByPermission(permission).forEach(r -> {
            if (first[0]) {
                query.append("role = '").append(r.role()).append("' ");
                first[0] = false;
            } else {
                query.append("OR role = '").append(r.role()).append("' ");
            }
        });
        query.append(");");
        return query.toString();
    }
}
