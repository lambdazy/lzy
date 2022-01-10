package ru.yandex.cloud.ml.platform.lzy.whiteboard.mem;

import io.grpc.Status;
import org.hibernate.Session;
import org.hibernate.query.Query;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
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

public class SessionHelper {
    public static List<WhiteboardModel> getWhiteboardModels(@Nullable String snapshotId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardModel> cr = cb.createQuery(WhiteboardModel.class);
        Root<WhiteboardModel> root = cr.from(WhiteboardModel.class);
        if (snapshotId != null) {
            cr.select(root).where(cb.equal(root.get("snapshotId"), snapshotId));
        }

        Query<WhiteboardModel> query = session.createQuery(cr);
        return query.getResultList();
    }

    @SuppressWarnings("unchecked")
    public static List<WhiteboardFieldModel> getNotCompletedWhiteboardFields(String whiteboardId, Session session) {
        String queryWhiteboardFieldRequest = "SELECT w FROM WhiteboardFieldModel w WHERE w.wbId = :wbId AND w.entryId is NULL";
        Query<WhiteboardFieldModel> queryWhiteboardField = session.createQuery(queryWhiteboardFieldRequest);
        queryWhiteboardField.setParameter("wbId", whiteboardId);
        return queryWhiteboardField.list();
    }

    @SuppressWarnings("unchecked")
    public static List<SnapshotEntryModel> getEntryDependencies(SnapshotEntryModel snapshotEntryModel, Session session) {
        String queryEntryDependenciesRequest = "SELECT s2 FROM SnapshotEntryModel s1 " +
                "JOIN EntryDependenciesModel e ON s1.entryId = e.entryIdTo " +
                "JOIN SnapshotEntryModel s2 ON s2.entryId = e.entryIdFrom " +
                "WHERE s1.snapshotId = :spId AND s1.entryId = :entryId";
        Query<SnapshotEntryModel> query = session.createQuery(queryEntryDependenciesRequest);
        query.setParameter("spId", snapshotEntryModel.getSnapshotId());
        query.setParameter("entryId", snapshotEntryModel.getEntryId());
        return query.list();
    }

    public static List<String> getEntryDependenciesName(SnapshotEntryModel snapshotEntryModel, Session session) {
        List<SnapshotEntryModel> entryModels = getEntryDependencies(snapshotEntryModel, session);
        return entryModels.stream()
                .map(SnapshotEntryModel::getEntryId)
                .collect(Collectors.toList());
    }

    public static Set<String> getWhiteboardFieldNames(String wbId, Session session) {
        List<WhiteboardFieldModel> results = getWhiteboardFields(wbId, session);
        Set<String> fieldNames = new HashSet<>();
        results.forEach(wbField -> fieldNames.add(wbField.getFieldName()));
        return fieldNames;
    }

    public static List<WhiteboardFieldModel> getWhiteboardFields(String wbId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardFieldModel> cr = cb.createQuery(WhiteboardFieldModel.class);
        Root<WhiteboardFieldModel> root = cr.from(WhiteboardFieldModel.class);
        cr.select(root).where(cb.equal(root.get("wbId"), wbId));

        Query<WhiteboardFieldModel> query = session.createQuery(cr);
        return query.getResultList();
    }

    @Nullable
    public static SnapshotEntryModel resolveSnapshotEntry(WhiteboardFieldModel wbFieldModel, Session session) {
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

    @SuppressWarnings("unchecked")
    public static List<WhiteboardFieldModel> getFieldDependencies(String wbId, String fieldName, Session session) {
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

    @Nullable
    public static SnapshotEntry getSnapshotEntry(WhiteboardFieldModel wbFieldModel, Snapshot snapshot, Session session) {
        SnapshotEntryModel snapshotEntryModel = resolveSnapshotEntry(wbFieldModel, session);
        if (snapshotEntryModel == null) {
            return null;
        }
        return new SnapshotEntry.Impl(snapshotEntryModel.getEntryId(), snapshot);
    }

    public static Whiteboard getWhiteboard(String wbId, Snapshot snapshot, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardFieldModel> cr = cb.createQuery(WhiteboardFieldModel.class);
        Root<WhiteboardFieldModel> root = cr.from(WhiteboardFieldModel.class);
        cr.select(root).where(cb.equal(root.get("wbId"), wbId));

        Query<WhiteboardFieldModel> query = session.createQuery(cr);
        List<WhiteboardFieldModel> results = query.getResultList();
        results.forEach(f -> ((Set<String>) new HashSet<String>()).add(f.getFieldName()));
        return new Whiteboard.Impl(URI.create(wbId), new HashSet<>(), snapshot);
    }

    @Nullable
    public static Snapshot getSnapshot(String spId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<SnapshotModel> cr = cb.createQuery(SnapshotModel.class);
        Root<SnapshotModel> root = cr.from(SnapshotModel.class);
        cr.select(root).where(cb.equal(root.get("snapshotId"), spId));

        Query<SnapshotModel> query = session.createQuery(cr);
        List<SnapshotModel> results = query.getResultList();
        if (results.isEmpty()) {
            return null;
        }
        return new Snapshot.Impl(URI.create(spId));
    }

    public static WhiteboardField getWhiteboardField(WhiteboardFieldModel wbFieldModel, Whiteboard whiteboard, Snapshot snapshot, Session session) {
        return new WhiteboardField.Impl(wbFieldModel.getFieldName(), getSnapshotEntry(wbFieldModel, snapshot, session), whiteboard);
    }

    @Nullable
    public static Snapshot resolveSnapshot(String spId, Session session) {
        SnapshotModel spModel = session.find(SnapshotModel.class, spId);
        if (spModel == null) {
            return null;
        }
        return new Snapshot.Impl(URI.create(spId));
    }

    @Nullable
    public static Whiteboard resolveWhiteboard(String wbId, Session session) {
        WhiteboardModel wbModel = session.find(WhiteboardModel.class, wbId);
        if (wbModel == null) {
            return null;
        }
        String spId = wbModel.getSnapshotId();
        return new Whiteboard.Impl(URI.create(wbId), SessionHelper.getWhiteboardFieldNames(wbId, session), resolveSnapshot(spId, session));
    }

    @Nullable
    public static WhiteboardStatus.State resolveWhiteboardState(String wbId, Session session) {
        WhiteboardModel wbModel = session.find(WhiteboardModel.class, wbId);
        if (wbModel == null) {
            return null;
        }
        return wbModel.getWbState();
    }

    @Nullable
    public static SnapshotEntryStatus resolveEntryStatus(Snapshot snapshot, String id, Session session) {
        String snapshotId = snapshot.id().toString();
        SnapshotEntryModel snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, id));
        if (snapshotEntryModel == null) {
            return null;
        }

        List<String> dependentEntryIds = SessionHelper.getEntryDependenciesName(snapshotEntryModel, session);
        SnapshotEntry entry = new SnapshotEntry.Impl(id, snapshot);
        return new SnapshotEntryStatus.Impl(snapshotEntryModel.isEmpty(),snapshotEntryModel.getEntryState(), entry,
                Set.copyOf(dependentEntryIds), URI.create(snapshotEntryModel.getStorageUri()));
    }
}
