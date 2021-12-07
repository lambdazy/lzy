package ru.yandex.cloud.ml.platform.lzy.whiteboard.mem;

import io.grpc.Status;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exception.WhiteboardException;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotEntryModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotOwnerModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardFieldModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardModel;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

import javax.annotation.Nullable;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Singleton
@Requires(beans = DbStorage.class)
public class WhiteboardRepositoryImpl implements WhiteboardRepository {
    @Inject
    DbStorage storage;

    private Set<String> getFieldNames(String wbId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardFieldModel> cr = cb.createQuery(WhiteboardFieldModel.class);
        Root<WhiteboardFieldModel> root = cr.from(WhiteboardFieldModel.class);
        cr.select(root).where(cb.equal(root.get("wbId"), wbId));

        Query<WhiteboardFieldModel> query = session.createQuery(cr);
        List<WhiteboardFieldModel> results = query.getResultList();
        Set<String> fieldNames = new HashSet<>();
        results.forEach(wbField -> fieldNames.add(wbField.getFieldName()));
        return fieldNames;
    }

    private List<String> getFieldDependencies(String wbId, String fieldName, Session session) {
//        Query query = session.createQuery("select m.name as employeeName, m.department.name as departmentName"
//                + " from com.baeldung.hibernate.entities.DeptEmployee m");
//        query.setResultTransformer(Transformers.aliasToBean(Result.class));
//        List<Result> results = query.list();
//        Result result = results.get(0);
//
//        String queryFieldDependenciesRequest = "SELECT f1.fieldName FROM WhiteboardModel w " +
//                "JOIN WhiteboardFieldModel f1 ON w.wbId = f1.wbId " +
//                "JOIN EntryDependenciesModel e ON e.snapshotId = w.snapshotId AND e.entryIdFrom = f1.entryId " +
//                "JOIN WhiteboardFieldModel f2 ON e.entryIdTo = f2.entryId " +
//                "WHERE w.wbId = :wbId AND f2.fieldName = :fName";
//        Query<String> query = session.createQuery(queryFieldDependenciesRequest);
//        query.setParameter("wbId", wbId);
//        query.setParameter("fName", fieldName);
//        List<String> result = query.list();
//        return result;

        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardFieldModel> cr = cb.createQuery(WhiteboardFieldModel.class);
        Root<WhiteboardFieldModel> root = cr.from(WhiteboardFieldModel.class);
        cr.select(root).where(cb.equal(root.get("wbId"), wbId));

        Query<WhiteboardFieldModel> query = session.createQuery(cr);
        List<WhiteboardFieldModel> results = query.getResultList();
        Set<String> fieldNames = new HashSet<>();
        results.forEach(wbField -> fieldNames.add(wbField.getFieldName()));
        return fieldNames;
    }

    @Nullable
    private Snapshot resolveSnapshot(String spId, Session session) {
        SnapshotOwnerModel spOwnerModel = session.find(SnapshotOwnerModel.class, spId);
        if (spOwnerModel == null) {
            return null;
        }
        return new Snapshot.Impl(URI.create(spId), URI.create(spOwnerModel.getOwnerId()));
    }

    @Nullable
    private Whiteboard resolveWhiteboard(String wbId, Session session) {
        WhiteboardModel wbModel = session.find(WhiteboardModel.class, wbId);
        if (wbModel == null) {
            return null;
        }
        String spId = wbModel.getSnapshotId();
        return new Whiteboard.Impl(URI.create(wbId), getFieldNames(wbId, session), resolveSnapshot(spId, session));
    }

    @Nullable
    private WhiteboardStatus.State resolveWhiteboardState(String wbId, Session session) {
        WhiteboardModel wbModel = session.find(WhiteboardModel.class, wbId);
        if (wbModel == null) {
            return null;
        }
        return wbModel.getWbState();
    }


    @Override
    public void create(Whiteboard whiteboard) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            WhiteboardModel wbModel = new WhiteboardModel(whiteboard.id().toString(),
                    WhiteboardStatus.State.CREATED, whiteboard.snapshot().id().toString());
            try {
                session.save(wbModel);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw new WhiteboardException(e);
            }
        }
    }

    @Nullable
    @Override
    public WhiteboardStatus resolveWhiteboard(URI id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            return new WhiteboardStatus.Impl(resolveWhiteboard(id.toString(), session),
                    resolveWhiteboardState(id.toString(), session));
        }
    }

    @Override
    public void add(WhiteboardField field) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            WhiteboardFieldModel wbModel = new WhiteboardFieldModel(field.whiteboard().id().toString(),
                    field.name(), field.entry().id());
            try {
                session.save(wbModel);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw new WhiteboardException(e);
            }
        }
    }

    @Override
    public Stream<WhiteboardField> dependent(WhiteboardField field) {
        try (Session session = storage.getSessionFactory().openSession()) {
            SnapshotEntry snapshotEntry = new SnapshotEntry.Impl
            return getFieldDependencies(field.whiteboard().id().toString(), field.name(), session).stream();
        }
    }

    @Override
    public Stream<WhiteboardField> fields(Whiteboard whiteboard) {
        return null;
    }
}
