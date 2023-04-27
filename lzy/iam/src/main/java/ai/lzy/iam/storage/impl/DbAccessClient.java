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
import java.util.stream.Collectors;

@Singleton
@Requires(beans = IamDataSource.class)
public class DbAccessClient {
    private static final Logger LOG = LogManager.getLogger(DbAccessClient.class);

    private static final String QUERY_FIND_USER_ROLE_RESOURCE = """
        SELECT user_id
        FROM user_resource_roles
        WHERE user_id = ? AND resource_id = ? AND role = ?
        """;

    private static final String QUERY_FIND_USER_RESOURCE = """
        SELECT user_id
        FROM user_resource_roles
        WHERE user_id = ? AND resource_id = ?
        """;

    private static final String QUERY_FIND_USER_ROLES_RESOURCE = """
        SELECT user_id
        FROM user_resource_roles
        WHERE user_id = ? AND resource_id = ? AND role IN (%s)
        """;

    private final IamDataSource storage;

    @Inject
    public DbAccessClient(IamDataSource storage) {
        this.storage = storage;
    }

    public boolean hasResourcePermission(Subject subject, String resourceId, AuthPermission permission)
            throws AuthException
    {
        if (Role.LZY_INTERNAL_USER.permissions().contains(permission)) {
            try (var conn = storage.connect();
                 var st = conn.prepareStatement(QUERY_FIND_USER_ROLE_RESOURCE))
            {
                int parameterIndex = 0;
                st.setString(++parameterIndex, subject.id());
                st.setString(++parameterIndex, Root.INSTANCE.resourceId());
                st.setString(++parameterIndex, Role.LZY_INTERNAL_USER.value());
                final ResultSet rs = st.executeQuery();
                if (rs.next()) {
                    LOG.debug("Internal access to resource::{}", resourceId);
                    return true;
                }
            } catch (SQLException e) {
                throw new AuthInternalException(e);
            }
        }

        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_FIND_USER_RESOURCE))
        {
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

        var roles = Role.rolesByPermission(permission).toList();
        var rolesPlaceholder = roles.stream().map(x -> "?").collect(Collectors.joining(","));

        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_FIND_USER_ROLES_RESOURCE.formatted(rolesPlaceholder)))
        {
            int parameterIndex = 0;
            st.setString(++parameterIndex, subject.id());
            st.setString(++parameterIndex, resourceId);
            for (var role : roles) {
                st.setString(++parameterIndex, role.value());
            }
            final ResultSet rs = st.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }

        return false;
    }
}
