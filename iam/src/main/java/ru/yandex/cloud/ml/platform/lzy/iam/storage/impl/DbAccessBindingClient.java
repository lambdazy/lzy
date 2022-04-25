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
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthInternalException;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.ResourceBinding;
import ru.yandex.cloud.ml.platform.lzy.model.iam.AccessBinding;
import ru.yandex.cloud.ml.platform.lzy.model.iam.AccessBindingDelta;
import ru.yandex.cloud.ml.platform.lzy.model.iam.AccessBindingDelta.AccessBindingAction;
import ru.yandex.cloud.ml.platform.lzy.model.iam.AuthResource;

@Singleton
@Requires(beans = DbStorage.class)
public class DbAccessBindingClient implements AccessBindingClient {

    @Inject
    DbStorage storage;

    @Override
    public Stream<AccessBinding> listAccessBindings(AuthResource resource) {
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
    public void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) {
        try {
            StringBuilder query = new StringBuilder();
            for (AccessBinding binding : accessBinding) {
                query.append(insertQuery(resource, binding.role(), binding.subject()));
            }
            final PreparedStatement st = storage.connect().prepareStatement(query.toString());
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }
    }

    @Override
    public void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas) {
        try {
            StringBuilder query = new StringBuilder();
            for (AccessBindingDelta binding : accessBindingDeltas) {
                if (binding.action() == AccessBindingAction.ADD) {
                    query.append(insertQuery(resource, binding.binding().role(), binding.binding().subject()));
                } else if (binding.action() == AccessBindingAction.REMOVE) {
                    query.append(deleteQuery(resource, binding.binding().role(), binding.binding().subject()));
                } else {
                    throw new RuntimeException("Unknown bindingDelta action:: " + binding.action());
                }
            }
            final PreparedStatement st = storage.connect().prepareStatement(query.toString());
            st.executeUpdate();
        } catch (SQLException e) {
            throw new AuthInternalException(e);
        }

    }

    private AccessBinding toAccessBinding(ResourceBinding model) {
        return new AccessBinding(model.role(), model.userId());
    }

    private String deleteQuery(AuthResource resource, String role, String subjectId) {
        return "DELETE from user_resource_roles"
                + " WHERE user_id = " + subjectId
                + " AND role = " + role
                + " AND resource_id  = " + resource.resourceId() + "; ";
    }

    private String insertQuery(AuthResource resource, String role, String subjectId) {
        return "INSERT INTO user_resource_roles"
                + " (user_id, resource_id, resource_type, role) values ("
                + subjectId + ", "
                + resource.resourceId() + ", "
                + resource.type() + ", "
                + role
                + ") ON CONFLICT DO NOTHING; ";
    }
}
