package ai.lzy.iam.storage.impl;

import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import ai.lzy.util.auth.exceptions.AuthNotFoundException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
@Requires(beans = IamDataSource.class)
public class DbAccessClient {
    private static final Logger LOG = LogManager.getLogger(DbAccessClient.class);

    @Inject
    private IamDataSource storage;

    public boolean hasResourcePermission(Subject subject, String resourceId, AuthPermission permission)
            throws AuthException
    {
        if (Role.LZY_INTERNAL_USER.permissions().contains(permission)) {
            try (var conn = storage.connect()) {
                var st = conn.prepareStatement("""
                    SELECT count(*) FROM user_resource_roles
                    WHERE user_id = ? AND resource_id = ? AND role = ?""");

                int parameterIndex = 0;
                st.setString(++parameterIndex, subject.id());
                st.setString(++parameterIndex, Root.INSTANCE.resourceId());
                st.setString(++parameterIndex, Role.LZY_INTERNAL_USER.value());
                final ResultSet rs = st.executeQuery();
                if (rs.next()) {
                    if (rs.getInt(1) > 0) {
                        LOG.info("Internal access to resource::{}", resourceId);
                        return true;
                    }
                }
            } catch (SQLException e) {
                throw new AuthInternalException(e);
            }
        }

        try (var conn = storage.connect()) {
            var st = conn.prepareStatement("""
                SELECT user_id FROM user_resource_roles
                WHERE user_id = ? AND resource_id = ?""");

            int parameterIndex = 0;
            st.setString(++parameterIndex, subject.id());
            st.setString(++parameterIndex, resourceId);
            final ResultSet rs = st.executeQuery();
            if (!rs.next()) {
                throw new AuthNotFoundException("Resource: " + resourceId + " not found");
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }

        try (var conn = storage.connect()) {
            var st = conn.prepareStatement("""
                SELECT user_id FROM user_resource_roles
                WHERE user_id = ? AND resource_id = ? AND""" + queryByPermission(permission));

            int parameterIndex = 0;
            st.setString(++parameterIndex, subject.id());
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
        StringBuilder query = new StringBuilder(" (");
        final boolean[] first = {true};
        Role.rolesByPermission(permission).forEach(r -> {
            if (first[0]) {
                query.append("role = '").append(r.value()).append("' ");
                first[0] = false;
            } else {
                query.append("OR role = '").append(r.value()).append("' ");
            }
        });
        query.append(");");
        return query.toString();
    }
}
