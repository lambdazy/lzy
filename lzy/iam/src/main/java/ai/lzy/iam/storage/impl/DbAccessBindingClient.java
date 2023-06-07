package ai.lzy.iam.storage.impl;

import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AccessBindingDelta.AccessBindingAction;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Singleton
@Requires(beans = IamDataSource.class)
public class DbAccessBindingClient {

    private static final String QUERY_LIST_ACCESS_BINDINGS = """
        SELECT r.user_id, r.resource_id, r.resource_type, r.role,
               u.user_type, u.auth_provider, u.provider_user_id
        FROM user_resource_roles AS r INNER JOIN users u on r.user_id = u.user_id
        WHERE r.resource_id = ?;
        """;

    private static final String QUERY_DELETE_ACCESS_BINDING = """
        DELETE FROM user_resource_roles
        WHERE user_id = ? AND role = ? AND resource_id  = ?;
        """;

    private static final String QUERY_CREATE_ACCESS_BINDING = """
        INSERT INTO user_resource_roles (user_id, resource_id, resource_type, role)
        VALUES (?, ?, ?, ?)
        ON CONFLICT DO NOTHING;
        """;


    private final IamDataSource storage;

    @Inject
    public DbAccessBindingClient(IamDataSource storage) {
        this.storage = storage;
    }

    public Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException {
        List<AccessBinding> bindings = new ArrayList<>();
        try (var conn = storage.connect();
             var selectSt = conn.prepareStatement(QUERY_LIST_ACCESS_BINDINGS))
        {
            int parameterIndex = 0;
            selectSt.setString(++parameterIndex, resource.resourceId());
            final ResultSet rs = selectSt.executeQuery();
            while (rs.next()) {
                var role = Role.of(rs.getString("role"));
                var subject = Subject.of(
                    rs.getString("user_id"),
                    SubjectType.valueOf(rs.getString("user_type")),
                    AuthProvider.valueOf(rs.getString("auth_provider")),
                    rs.getString("provider_user_id"));
                var accessBinding = new AccessBinding(role, subject);
                bindings.add(accessBinding);
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
        return bindings.stream();
    }

    public void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) throws AuthException {
        try (final Connection connection = storage.connect()) {
            StringBuilder query = new StringBuilder();
            for (AccessBinding ignored : accessBinding) {
                query.append(QUERY_CREATE_ACCESS_BINDING);
            }
            try (final PreparedStatement st = connection.prepareStatement(query.toString())) {
                int parameterIndex = 0;
                for (AccessBinding binding : accessBinding) {
                    st.setString(++parameterIndex, binding.subject().id());
                    st.setString(++parameterIndex, resource.resourceId());
                    st.setString(++parameterIndex, resource.type());
                    st.setString(++parameterIndex, binding.role().value());
                }
                st.executeUpdate();
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    public void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas)
        throws AuthException
    {
        try (final Connection connection = storage.connect()) {
            StringBuilder query = new StringBuilder();
            for (AccessBindingDelta binding : accessBindingDeltas) {
                if (binding.action() == AccessBindingAction.ADD) {
                    query.append(QUERY_CREATE_ACCESS_BINDING);
                } else if (binding.action() == AccessBindingAction.REMOVE) {
                    query.append(QUERY_DELETE_ACCESS_BINDING);
                } else {
                    throw new RuntimeException("Unknown bindingDelta action:: " + binding.action());
                }
            }
            try (final PreparedStatement st = connection.prepareStatement(query.toString())) {
                int parameterIndex = 0;
                for (AccessBindingDelta binding : accessBindingDeltas) {
                    if (binding.action() == AccessBindingAction.ADD) {
                        st.setString(++parameterIndex, binding.binding().subject().id());
                        st.setString(++parameterIndex, resource.resourceId());
                        st.setString(++parameterIndex, resource.type());
                        st.setString(++parameterIndex, binding.binding().role().value());
                    } else if (binding.action() == AccessBindingAction.REMOVE) {
                        st.setString(++parameterIndex, binding.binding().subject().id());
                        st.setString(++parameterIndex, binding.binding().role().value());
                        st.setString(++parameterIndex, resource.resourceId());
                    } else {
                        throw new RuntimeException("Unknown bindingDelta action:: " + binding.action());
                    }
                }
                st.executeUpdate();
            }
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }
}
