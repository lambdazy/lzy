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
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.models.ResourceBindingModel;
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
    public List<AccessBinding> listAccessBindings(AuthResource resource) {
        List<AccessBinding> bindings = new ArrayList<>();
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                List<ResourceBindingModel> rs = session.createQuery(
                        "SELECT r FROM ResourceBindingModel r "
                            + "WHERE r.resourceId = :resourceId ",
                        ResourceBindingModel.class
                    ).setParameter("resourceId", resource.resourceId())
                    .getResultList();
                bindings.addAll(rs.stream().map(this::toAccessBinding).collect(Collectors.toList()));
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
            }
        }
        return bindings;
    }

    @Override
    public void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                StringBuilder query = new StringBuilder();
                for (AccessBinding binding : accessBinding) {
                    query.append(insertQuery(resource, binding.role(), binding.subject()));
                }
                session.createSQLQuery(query.toString()).executeUpdate();
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    @Override
    public void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas) {
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
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    private AccessBinding toAccessBinding(ResourceBindingModel model) {
        return new AccessBinding(model.role(), model.userId());
    }

    private String deleteQuery(AuthResource resource, String role, String subjectId) {
        return "DELETE from ResourceBindingModel r"
            + " WHERE r.userId = " + subjectId
            + " AND r.role = " + role
            + " AND r.resourceId  = " + resource.resourceId() + "; ";
    }

    private String insertQuery(AuthResource resource, String role, String subjectId) {
        return "INSERT INTO ResourceBindingModel r "
            + " (r.userId, r.resourceId, r.resourceType, r.role) values ("
            + subjectId + ", "
            + resource.resourceId() + ", "
            + resource.type() + ", "
            + role
            + ") ON CONFLICT DO NOTHING; ";
    }
}
