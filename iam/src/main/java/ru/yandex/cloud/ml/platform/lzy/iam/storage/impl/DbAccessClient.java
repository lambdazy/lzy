package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.Role;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.db.models.ResourceBindingModel;

@Singleton
@Requires(beans = DbStorage.class)
public class DbAccessClient implements AccessClient {

    @Inject
    DbStorage storage;

    @Override
    public boolean hasResourcePermission(String userId, String resourceId, AuthPermission permission) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                List<ResourceBindingModel> bindings = session.createQuery(
                        "SELECT r FROM ResourceBindingModel r "
                            + "WHERE r.userId = :userId "
                            + "AND r.resourceId = :resourceId "
                            + "AND " + queryByPermission(permission),
                        ResourceBindingModel.class
                    ).setParameter("userId", userId)
                    .setParameter("resourceId", resourceId)
                    .getResultList();
                if (bindings.size() > 0) {
                    return true;
                }
            } catch (Exception e) {
                tx.rollback();
            }
        }
        return false;
    }

    private String queryByPermission(AuthPermission permission) {
        StringBuilder query = new StringBuilder("(");
        final boolean[] first = {true};
        Role.rolesByPermission(permission).forEach(r -> {
            if (first[0]) {
                query.append("r.role = '").append(r.role()).append("' ");
                first[0] = false;
            } else {
                query.append("OR r.role = '").append(r.role()).append("' ");
            }
        });
        query.append(")");
        return query.toString();
    }
}
