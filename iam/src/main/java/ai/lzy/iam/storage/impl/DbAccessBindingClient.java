package ai.lzy.iam.storage.impl;

import ai.lzy.iam.resources.Role;
import ai.lzy.iam.storage.db.IamDataSource;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AccessBindingDelta.AccessBindingAction;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.storage.db.ResourceBinding;

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

    @Inject
    private IamDataSource storage;

    public Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException {
        List<AccessBinding> bindings = new ArrayList<>();
        try (final PreparedStatement st = storage.connect()
                .prepareStatement("SELECT * FROM user_resource_roles " + "WHERE resource_id = ? ")) {
            int parameterIndex = 0;
            st.setString(++parameterIndex, resource.resourceId());
            final ResultSet rs = st.executeQuery();
            while (rs.next()) {
                bindings.add(toAccessBinding(ResourceBinding.fromResultSet(rs)));
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
                query.append(insertQuery());
            }
            final PreparedStatement st = connection.prepareStatement(query.toString());
            int parameterIndex = 0;
            for (AccessBinding binding : accessBinding) {
                st.setString(++parameterIndex, binding.subject().id());
                st.setString(++parameterIndex, resource.resourceId());
                st.setString(++parameterIndex, resource.type());
                st.setString(++parameterIndex, binding.role().value());
            }
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    public void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas)
            throws AuthException {
        try (final Connection connection = storage.connect()) {
            StringBuilder query = new StringBuilder();
            for (AccessBindingDelta binding : accessBindingDeltas) {
                if (binding.action() == AccessBindingAction.ADD) {
                    query.append(insertQuery());
                } else if (binding.action() == AccessBindingAction.REMOVE) {
                    query.append(deleteQuery());
                } else {
                    throw new RuntimeException("Unknown bindingDelta action:: " + binding.action());
                }
            }
            final PreparedStatement st = connection.prepareStatement(query.toString());
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
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }

    }

    private AccessBinding toAccessBinding(ResourceBinding model) {
        return new AccessBinding(Role.of(model.role()), new User(model.userId()));
    }

    private String deleteQuery() {
        return "DELETE from user_resource_roles" + " WHERE user_id = ?" + " AND role = ?" + " AND resource_id  = ?; ";
    }

    private String insertQuery() {
        return "INSERT INTO user_resource_roles"
                + " (user_id, resource_id, resource_type, role) values ("
                + "?, ?, ?, ?"
                + ") ON CONFLICT DO NOTHING; ";
    }
}
