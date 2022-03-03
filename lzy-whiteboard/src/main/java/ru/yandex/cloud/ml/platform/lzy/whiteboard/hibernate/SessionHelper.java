package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate;

import java.net.URI;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.persistence.TemporalType;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import org.hibernate.Session;
import org.hibernate.query.Query;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotEntryModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardFieldModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardTagModel;

public class SessionHelper {

    public static List<SnapshotEntryModel> getSnapshotEntries(String snapshotId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<SnapshotEntryModel> cr = cb.createQuery(SnapshotEntryModel.class);
        Root<SnapshotEntryModel> root = cr.from(SnapshotEntryModel.class);
        cr.select(root).where(cb.equal(root.get("snapshotId"), snapshotId));

        Query<SnapshotEntryModel> query = session.createQuery(cr);
        return query.getResultList();
    }

    public static List<WhiteboardModel> getWhiteboardModels(String snapshotId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardModel> cr = cb.createQuery(WhiteboardModel.class);
        Root<WhiteboardModel> root = cr.from(WhiteboardModel.class);
        cr.select(root).where(cb.equal(root.get("snapshotId"), snapshotId));

        Query<WhiteboardModel> query = session.createQuery(cr);
        return query.getResultList();
    }

    public static long getNumEntriesWithStateForWhiteboard(String whiteboardId, SnapshotEntryStatus.State state,
        Session session) {
        String queryWhiteboardFieldRequest = "SELECT count(*) FROM WhiteboardFieldModel w "
            + "JOIN SnapshotEntryModel s ON w.entryId = s.entryId "
            + "WHERE w.wbId = :wbId AND s.entryState = :state";
        //noinspection unchecked
        Query<Long> queryWhiteboardField = session.createQuery(queryWhiteboardFieldRequest);
        queryWhiteboardField.setParameter("wbId", whiteboardId);
        queryWhiteboardField.setParameter("state", state);
        return queryWhiteboardField.getSingleResult();
    }

    public static long getWhiteboardFieldsNum(String whiteboardId, Session session) {
        String queryWhiteboardFieldRequest = "SELECT count(*) FROM WhiteboardFieldModel w "
            + "WHERE w.wbId = :wbId";
        //noinspection unchecked
        Query<Long> queryWhiteboardField = session.createQuery(queryWhiteboardFieldRequest);
        queryWhiteboardField.setParameter("wbId", whiteboardId);
        return queryWhiteboardField.getSingleResult();
    }

    @SuppressWarnings("unchecked")
    public static List<SnapshotEntryModel> getEntryDependencies(SnapshotEntryModel snapshotEntryModel,
        Session session) {
        String queryEntryDependenciesRequest = "SELECT s2 FROM SnapshotEntryModel s1 "
            + "JOIN EntryDependenciesModel e ON s1.entryId = e.entryIdTo "
            + "JOIN SnapshotEntryModel s2 ON s2.entryId = e.entryIdFrom "
            + "WHERE s1.snapshotId = :spId AND s1.entryId = :entryId";
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

    public static Set<String> getWhiteboardTags(String wbId, Session session) {
        List<WhiteboardTagModel> results = getWhiteboardTagModels(wbId, session);
        Set<String> tags = new HashSet<>();
        results.forEach(tag -> tags.add(tag.getTag()));
        return tags;
    }

    public static List<WhiteboardFieldModel> getWhiteboardFields(String wbId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardFieldModel> cr = cb.createQuery(WhiteboardFieldModel.class);
        Root<WhiteboardFieldModel> root = cr.from(WhiteboardFieldModel.class);
        cr.select(root).where(cb.equal(root.get("wbId"), wbId));

        Query<WhiteboardFieldModel> query = session.createQuery(cr);
        return query.getResultList();
    }

    public static List<WhiteboardTagModel> getWhiteboardTagModels(String wbId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<WhiteboardTagModel> cr = cb.createQuery(WhiteboardTagModel.class);
        Root<WhiteboardTagModel> root = cr.from(WhiteboardTagModel.class);
        cr.select(root).where(cb.equal(root.get("wbId"), wbId));

        Query<WhiteboardTagModel> query = session.createQuery(cr);
        return query.getResultList();
    }

    @Nullable
    public static SnapshotEntryModel resolveSnapshotEntry(String snapshotId, String entryId, Session session) {
        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<SnapshotEntryModel> cr = cb.createQuery(SnapshotEntryModel.class);
        Root<SnapshotEntryModel> root = cr.from(SnapshotEntryModel.class);
        cr.select(root)
            .where(cb.equal(root.get("snapshotId"), snapshotId))
            .where(cb.equal(root.get("entryId"), entryId));

        Query<SnapshotEntryModel> query = session.createQuery(cr);
        List<SnapshotEntryModel> results = query.getResultList();
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0);
    }

    @SuppressWarnings("unchecked")
    public static List<WhiteboardFieldModel> getFieldDependencies(String wbId, String fieldName, Session session) {
        String queryFieldDependenciesRequest = "SELECT f1 FROM WhiteboardModel w "
            + "JOIN WhiteboardFieldModel f1 ON w.wbId = f1.wbId "
            + "JOIN EntryDependenciesModel e ON e.snapshotId = w.snapshotId AND e.entryIdFrom = f1.entryId "
            + "JOIN WhiteboardFieldModel f2 ON e.entryIdTo = f2.entryId "
            + "WHERE w.wbId = :wbId AND f2.fieldName = :fName";
        Query<WhiteboardFieldModel> query = session.createQuery(queryFieldDependenciesRequest);
        query.setParameter("wbId", wbId);
        query.setParameter("fName", fieldName);
        return query.list();
    }

    @Nullable
    public static SnapshotEntry getSnapshotEntry(String entryId, Snapshot snapshot, Session session) {
        SnapshotEntryModel snapshotEntryModel = resolveSnapshotEntry(snapshot.id().toString(), entryId, session);
        if (snapshotEntryModel == null) {
            return null;
        }
        return new SnapshotEntry.Impl(snapshotEntryModel.getEntryId(), snapshot);
    }

    public static Whiteboard getWhiteboard(String wbId, Snapshot snapshot, Session session) {
        WhiteboardModel wbModel = session.find(WhiteboardModel.class, wbId);
        if (wbModel == null) {
            return null;
        }
        Set<String> fieldNames = getWhiteboardFieldNames(wbId, session);
        Set<String> tags = getWhiteboardTags(wbId, session);
        return new Whiteboard.Impl(URI.create(wbId), fieldNames, snapshot, tags,
            wbModel.getNamespace(), wbModel.getCreationDateUTC());
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
        return new Snapshot.Impl(URI.create(spId), URI.create(results.get(0).getUid()));
    }

    public static WhiteboardField getWhiteboardField(WhiteboardFieldModel wbFieldModel,
        Whiteboard whiteboard, Snapshot snapshot, Session session) {
        return new WhiteboardField.Impl(wbFieldModel.getFieldName(),
            wbFieldModel.getEntryId() == null ? null
                : getSnapshotEntry(wbFieldModel.getEntryId(), snapshot, session), whiteboard);
    }

    @Nullable
    public static Snapshot resolveSnapshot(String spId, Session session) {
        SnapshotModel spModel = session.find(SnapshotModel.class, spId);
        if (spModel == null) {
            return null;
        }
        return new Snapshot.Impl(URI.create(spId), URI.create(spModel.getUid()));
    }

    @Nullable
    public static Whiteboard resolveWhiteboard(String wbId, Session session) {
        WhiteboardModel wbModel = session.find(WhiteboardModel.class, wbId);
        if (wbModel == null) {
            return null;
        }
        String spId = wbModel.getSnapshotId();
        return new Whiteboard.Impl(
            URI.create(wbId),
            SessionHelper.getWhiteboardFieldNames(wbId, session),
            resolveSnapshot(spId, session),
            SessionHelper.getWhiteboardTags(wbId, session),
            wbModel.getNamespace(),
            wbModel.getCreationDateUTC()
        );
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
        return new SnapshotEntryStatus.Impl(snapshotEntryModel.isEmpty(), snapshotEntryModel.getEntryState(), entry,
            Set.copyOf(dependentEntryIds),
            snapshotEntryModel.getStorageUri() == null ? null : URI.create(snapshotEntryModel.getStorageUri()));
    }

    public static List<String> resolveWhiteboardIds(String namespace, List<String> tags,
        Date fromDateUTCIncluded, Date toDateUTCExcluded, Session session) {
        String whiteboardsByNameAndTagsRequest;
        if (tags.isEmpty()) {
            whiteboardsByNameAndTagsRequest = "SELECT w.wbId FROM WhiteboardModel w "
                + "WHERE w.namespace = :namespace AND w.creationDateUTC >= :dateFrom AND w.creationDateUTC < :dateTo "
                + "AND w.wbState = :state";
        } else {
            whiteboardsByNameAndTagsRequest = "SELECT w.wbId FROM WhiteboardModel w "
                + "JOIN WhiteboardTagModel t ON w.wbId = t.wbId "
                + "WHERE w.namespace = :namespace AND t.tag in (:tags) AND "
                + "w.creationDateUTC >= :dateFrom AND w.creationDateUTC < :dateTo "
                + "AND w.wbState = :state "
                + "GROUP BY w.wbId "
                + "HAVING count(*) >= :tagsSize ";
        }
        //noinspection unchecked
        Query<String> query = session.createQuery(whiteboardsByNameAndTagsRequest);
        query.setParameter("namespace", namespace);
        query.setParameter("dateFrom", fromDateUTCIncluded, TemporalType.TIMESTAMP);
        query.setParameter("dateTo", toDateUTCExcluded, TemporalType.TIMESTAMP);
        query.setParameter("state", State.COMPLETED);
        if (!tags.isEmpty()) {
            query.setParameter("tagsSize", (long) tags.size());
            query.setParameterList("tags", tags);
        }
        return query.list();
    }
}
