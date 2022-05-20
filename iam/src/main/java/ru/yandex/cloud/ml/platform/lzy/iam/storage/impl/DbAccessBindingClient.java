package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessBindingClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthInternalException;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.ResourceBinding;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBinding;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBindingDelta;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBindingDelta.AccessBindingAction;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;

@Singleton
@Requires(beans = DbStorage.class)
public class DbAccessBindingClient implements AccessBindingClient {

    @Inject
    private DbStorage storage;

    @Override
    public Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException {
        List<AccessBinding> bindings = new ArrayList<>();
        try (final PreparedStatement st = storage.connect().prepareStatement(
                "SELECT * FROM user_resource_roles "
                        + "WHERE resource_id = ? "
        )) {
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

    @Override
    public void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) throws AuthException {
        try {
            StringBuilder query = new StringBuilder();
            for (AccessBinding ignored : accessBinding) {
                query.append(insertQuery());
            }
            final PreparedStatement st = storage.connect().prepareStatement(query.toString());
            int parameterIndex = 0;
            for (AccessBinding binding : accessBinding) {
                st.setString(++parameterIndex, binding.subject());
                st.setString(++parameterIndex, resource.resourceId());
                st.setString(++parameterIndex, resource.type());
                st.setString(++parameterIndex, binding.role());
            }
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    @Override
    public void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas)
            throws AuthException {
        try {
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
            final PreparedStatement st = storage.connect().prepareStatement(query.toString());
            int parameterIndex = 0;
            for (AccessBindingDelta binding : accessBindingDeltas) {
                if (binding.action() == AccessBindingAction.ADD) {
                    st.setString(++parameterIndex, binding.binding().subject());
                    st.setString(++parameterIndex, resource.resourceId());
                    st.setString(++parameterIndex, resource.type());
                    st.setString(++parameterIndex, binding.binding().role());
                } else if (binding.action() == AccessBindingAction.REMOVE) {
                    st.setString(++parameterIndex, binding.binding().subject());
                    st.setString(++parameterIndex, binding.binding().role());
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
        return new AccessBinding(model.role(), model.userId());
    }

    private String deleteQuery() {
        return "DELETE from user_resource_roles"
                + " WHERE user_id = ?"
                + " AND role = ?"
                + " AND resource_id  = ?; ";
    }

    private String insertQuery() {
        return "INSERT INTO user_resource_roles"
                + " (user_id, resource_id, resource_type, role) values ("
                + "?, ?, ?, ?"
                + ") ON CONFLICT DO NOTHING; ";
    }
}
