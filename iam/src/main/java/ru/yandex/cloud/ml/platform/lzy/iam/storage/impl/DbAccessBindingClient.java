package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessBindingClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessBindingClient.AccessBindingDelta.AccessBindingAction;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResources;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.models.ResourceBindingModel;

@Singleton
@Requires(beans = DbStorage.class)
public class DbAccessBindingClient implements AccessBindingClient {

    @Inject
    DbStorage storage;

    @Override
    public List<AccessBinding> listAccessBindings(AuthResources resource) {
        List<AccessBinding> bindings = new ArrayList<>();
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                List<ResourceBindingModel> rs = session.createQuery(
                        "SELECT * FROM user_resource_roles "
                            + "WHERE resource_id = :resourceId ",
                        ResourceBindingModel.class
                    ).setParameter("resourceId", resource.resourceId())
                    .getResultList();
                bindings.addAll(rs.stream().map(this::toAccessBinding).collect(Collectors.toList()));
            } catch (Exception e) {
                tx.rollback();
            }
        }
        return bindings;
    }

    @Override
    public void setAccessBindings(AuthResources resource, List<AccessBinding> accessBinding) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                StringBuilder query = new StringBuilder();
                for (AccessBinding binding : accessBinding) {
                    query.append(insertQuery(resource, binding.role(), binding.subject()));
                }
                session.createSQLQuery(query.toString()).executeUpdate();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    @Override
    public void updateAccessBindings(AuthResources resource, List<AccessBindingDelta> accessBindingDeltas) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
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
                session.createSQLQuery(query.toString()).executeUpdate();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    private AccessBinding toAccessBinding(ResourceBindingModel model) {
        return new AccessBinding(model.role(), model.userId());
    }

    private String deleteQuery(AuthResources resource, String role, String subjectId) {
        return "DELETE from user_resource_roles"
            + " WHERE user_id = " + subjectId
            + " AND role = " + role
            + " AND resource_id  = " + resource.resourceId() + "; ";
    }

    private String insertQuery(AuthResources resource, String role, String subjectId) {
        return "INSERT INTO user_resource_roles "
            + " (user_id, resource_id, resource_type, role) values ("
            + subjectId + ", "
            + resource.resourceId() + ", "
            + resource.type() + ", "
            + role
            + ") ON CONFLICT DO NOTHING; ";
    }
}
