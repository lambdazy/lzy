package ru.yandex.cloud.ml.platform.lzy.whiteboard.mem;

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
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.*;

import javax.annotation.Nullable;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
@Requires(beans = DbStorage.class)
public class WhiteboardRepositoryImpl implements WhiteboardRepository {
    @Inject
    DbStorage storage;

    private Set<String> getWhiteboardFieldNames(String wbId, Session session) {
        List<WhiteboardFieldModel> results = getWhiteboardFields(wbId, session);
        Set<String> fieldNames = new HashSet<>();
        results.forEach(wbField -> fieldNames.add(wbField.getFieldName()));
        return fieldNames;
    }

    private List<WhiteboardFieldModel> getWhiteboardFields(String wbId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardFieldModel> cr = cb.createQuery(WhiteboardFieldModel.class);
        Root<WhiteboardFieldModel> root = cr.from(WhiteboardFieldModel.class);
        cr.select(root).where(cb.equal(root.get("wbId"), wbId));

        Query<WhiteboardFieldModel> query = session.createQuery(cr);
        return query.getResultList();
    }

    private SnapshotEntryModel resolveSnapshotEntry(WhiteboardFieldModel wbFieldModel, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<SnapshotEntryModel> cr = cb.createQuery(SnapshotEntryModel.class);
        Root<SnapshotEntryModel> root = cr.from(SnapshotEntryModel.class);
        cr.select(root)
                .where(cb.equal(root.get("snapshotId"), wbFieldModel.getSnapshotId()))
                .where(cb.equal(root.get("entryId"), wbFieldModel.getEntryId()));

        Query<SnapshotEntryModel> query = session.createQuery(cr);
        List<SnapshotEntryModel> results = query.getResultList();
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0);
    }

    private List<SnapshotEntryModel> getEntryDependencies(SnapshotEntryModel snapshotEntryModel, Session session) {
        String queryEntryDependenciesRequest = "SELECT s2 FROM SnapshotEntryModel s1 " +
                "JOIN EntryDependenciesModel e ON s1.entryId = e.entryIdTo " +
                "JOIN SnapshotEntryModel s2 ON s2.entryId = e.entryIdFrom " +
                "WHERE s1.snapshotId = :spId AND s1.entryId = :entryId";
        Query<SnapshotEntryModel> query = session.createQuery(queryEntryDependenciesRequest);
        query.setParameter("spId", snapshotEntryModel.getSnapshotId());
        query.setParameter("entryId", snapshotEntryModel.getEntryId());
        return query.list();
    }

    private List<WhiteboardFieldModel> getFieldDependencies(String wbId, String fieldName, Session session) {
        String queryFieldDependenciesRequest = "SELECT f1 FROM WhiteboardModel w " +
                "JOIN WhiteboardFieldModel f1 ON w.wbId = f1.wbId " +
                "JOIN EntryDependenciesModel e ON e.snapshotId = w.snapshotId AND e.entryIdFrom = f1.entryId " +
                "JOIN WhiteboardFieldModel f2 ON e.entryIdTo = f2.entryId " +
                "WHERE w.wbId = :wbId AND f2.fieldName = :fName";
        Query<WhiteboardFieldModel> query = session.createQuery(queryFieldDependenciesRequest);
        query.setParameter("wbId", wbId);
        query.setParameter("fName", fieldName);
        return query.list();
    }

    private SnapshotEntry getSnapshotEntry(WhiteboardFieldModel wbFieldModel, Snapshot snapshot, Session session) {
        SnapshotEntryModel snapshotEntryModel = resolveSnapshotEntry(wbFieldModel, session);
        if (snapshotEntryModel == null) {
            return null;
        }
        Set<String> entryIdsDeps = new HashSet<>();
        List<SnapshotEntryModel> deps = getEntryDependencies(snapshotEntryModel, session);
        deps.stream().forEach((d) -> entryIdsDeps.add(d.getEntryId()));
        return new SnapshotEntry.Impl(snapshotEntryModel.getEntryId(), URI.create(snapshotEntryModel.getStorageUri()), entryIdsDeps, snapshot);
    }

    private Whiteboard getWhiteboard(String wbId, Snapshot snapshot, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardFieldModel> cr = cb.createQuery(WhiteboardFieldModel.class);
        Root<WhiteboardFieldModel> root = cr.from(WhiteboardFieldModel.class);
        cr.select(root).where(cb.equal(root.get("wbId"), wbId));

        Query<WhiteboardFieldModel> query = session.createQuery(cr);
        List<WhiteboardFieldModel> results = query.getResultList();
        results.forEach(f -> ((Set<String>) new HashSet<String>()).add(f.getFieldName()));
        return new Whiteboard.Impl(URI.create(wbId), new HashSet<>(), snapshot);
    }

    private Snapshot getSnapshot(String spId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<SnapshotOwnerModel> cr = cb.createQuery(SnapshotOwnerModel.class);
        Root<SnapshotOwnerModel> root = cr.from(SnapshotOwnerModel.class);
        cr.select(root).where(cb.equal(root.get("snapshotId"), spId));

        Query<SnapshotOwnerModel> query = session.createQuery(cr);
        List<SnapshotOwnerModel> results = query.getResultList();
        if (results.isEmpty()) {
            return null;
        }
        return new Snapshot.Impl(URI.create(spId), URI.create(results.get(0).getOwnerId()));
    }

    private WhiteboardField getWhiteboardField(WhiteboardFieldModel wbFieldModel, Whiteboard whiteboard, Snapshot snapshot, Session session) {
        return new WhiteboardField.Impl(wbFieldModel.getFieldName(), getSnapshotEntry(wbFieldModel, snapshot, session), whiteboard);
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
        return new Whiteboard.Impl(URI.create(wbId), getWhiteboardFieldNames(wbId, session), resolveSnapshot(spId, session));
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
            Snapshot snapshot = getSnapshot(field.whiteboard().snapshot().id().toString(), session);
            Whiteboard whiteboard = getWhiteboard(field.whiteboard().id().toString(), snapshot, session);
            List<WhiteboardFieldModel> wbFieldModelList = getFieldDependencies(field.whiteboard().id().toString(), field.name(), session);
            if (!wbFieldModelList.isEmpty()) {
                System.out.println("WWWWWWWW1 " + field.name() + " " + wbFieldModelList.get(0).getFieldName());
            }
            List<WhiteboardField> result = wbFieldModelList.stream()
                    .map(w -> getWhiteboardField(w, whiteboard, snapshot, session))
                    .collect(Collectors.toList());
            if (!result.isEmpty()) {
                System.out.println("WWWWWWWW2 " + field.name() + " " + result.get(0).name());
            }
            return result.stream();
        }
    }

    @Override
    public Stream<WhiteboardField> fields(Whiteboard whiteboard) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Snapshot snapshot = getSnapshot(whiteboard.id().toString(), session);
            List<WhiteboardFieldModel> wbFieldModelList = getWhiteboardFields(whiteboard.id().toString(), session);
            List<WhiteboardField> result = wbFieldModelList.stream()
                    .map(w -> getWhiteboardField(w, whiteboard, snapshot, session))
                    .collect(Collectors.toList());
            return result.stream();
        }
    }
}
