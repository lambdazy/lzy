package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate;

import io.grpc.Status;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry.Impl;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus.State;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.EntryDependenciesModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotEntryModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardModel;

@Singleton
@Requires(beans = DbStorage.class)
public class DbSnapshotRepository implements SnapshotRepository {

    private static final Logger LOG = LogManager.getLogger(DbSnapshotRepository.class);

    @Inject
    DbStorage storage;

    @Override
    public SnapshotStatus create(Snapshot snapshot) throws RuntimeException {
        try (Session session = storage.getSessionFactory().openSession()) {
            String snapshotId = snapshot.id().toString();
            SnapshotModel snapshotStatus = session.find(SnapshotModel.class, snapshotId);
            if (snapshotStatus != null) {
                throw new RuntimeException("Snapshot with id " + snapshot.id() + " already exists");
            }

            Transaction tx = session.beginTransaction();
            snapshotStatus = new SnapshotModel(snapshotId, SnapshotStatus.State.CREATED,
                snapshot.uid().toString(), snapshot.creationDateUTC(), snapshot.workflowName());
            try {
                session.save(snapshotStatus);
                tx.commit();
                return new SnapshotStatus.Impl(snapshot, snapshotStatus.getSnapshotState());
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SnapshotStatus createFromSnapshot(String fromSnapshotId, Snapshot snapshot) throws RuntimeException {
        try (Session session = storage.getSessionFactory().openSession()) {
            SnapshotModel fromSnapshotModel = session.find(SnapshotModel.class, fromSnapshotId);
            if (fromSnapshotModel == null) {
                throw new RuntimeException("Snapshot with id "
                    + fromSnapshotId + " does not exists; snapshot with id "
                    + snapshot.id().toString() + " could not be created"
                );
            }

            String snapshotId = snapshot.id().toString();
            SnapshotModel snapshotModel = session.find(SnapshotModel.class, snapshotId);
            if (snapshotModel != null) {
                throw new RuntimeException("Snapshot with id " + snapshotId + " already exists");
            }

            Transaction tx = session.beginTransaction();
            snapshotModel = new SnapshotModel(snapshotId, SnapshotStatus.State.CREATED,
                snapshot.uid().toString(), snapshot.creationDateUTC(), snapshot.workflowName());
            List<SnapshotEntryModel> fromSnapshotEntries = SessionHelper.getSnapshotEntries(fromSnapshotId, session);
            List<SnapshotEntryModel> snapshotEntries = fromSnapshotEntries.stream()
                .filter(fromSnapshotEntry -> Objects.equals(fromSnapshotEntry.getEntryState(), State.FINISHED))
                .map(fromSnapshotEntry -> new SnapshotEntryModel(snapshotId, fromSnapshotEntry.getEntryId(),
                    fromSnapshotEntry.getStorageUri(), fromSnapshotEntry.isEmpty(), fromSnapshotEntry.getEntryState()))
                .collect(Collectors.toList());
            // TODO: manage entry dependencies
            try {
                session.save(snapshotModel);
                for (var entryModel : snapshotEntries) {
                    session.save(entryModel);
                }
                tx.commit();
                return new SnapshotStatus.Impl(snapshot, snapshotModel.getSnapshotState());
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Nullable
    @Override
    public SnapshotStatus resolveSnapshot(URI id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            SnapshotModel snapshotModel = session.find(SnapshotModel.class, id.toString());
            if (snapshotModel == null) {
                return null;
            }
            return new SnapshotStatus.Impl(
                new Snapshot.Impl(
                    id,
                    URI.create(snapshotModel.getUid()),
                    snapshotModel.creationDateUTC(),
                    snapshotModel.workflowName()
                ),
                snapshotModel.getSnapshotState());
        }
    }

    @Override
    public void finalize(Snapshot snapshot) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String snapshotId = snapshot.id().toString();
            SnapshotModel snapshotModel = session.find(SnapshotModel.class, snapshotId);
            if (snapshotModel == null) {
                throw new RuntimeException(Status.NOT_FOUND.asException());
            }
            if (snapshotModel.getSnapshotState() == SnapshotStatus.State.ERRORED
                || snapshotModel.getSnapshotState() == SnapshotStatus.State.FINALIZED) {
                return;
            }
            List<SnapshotEntryModel> snapshotEntries = SessionHelper.getSnapshotEntries(snapshotId, session);
            for (SnapshotEntryModel spEntry : snapshotEntries) {
                if (spEntry.getEntryState() != State.FINISHED || spEntry.getStorageUri() == null) {
                    LOG.info("Error in entry {}: status {}", spEntry.getEntryId(), spEntry.getEntryState());
                    spEntry.setEntryState(State.ERRORED);
                    snapshotModel.setSnapshotState(SnapshotStatus.State.ERRORED);
                }
            }
            if (snapshotModel.getSnapshotState() != SnapshotStatus.State.ERRORED) {
                snapshotModel.setSnapshotState(SnapshotStatus.State.FINALIZED);
            }

            List<WhiteboardModel> whiteboards = SessionHelper.getWhiteboardModels(snapshotId,
                session);
            for (WhiteboardModel wbModel : whiteboards) {
                if (snapshotModel.getSnapshotState() == SnapshotStatus.State.ERRORED) {
                    wbModel.setWbState(WhiteboardStatus.State.ERRORED);
                    continue;
                }
                final long fieldsNum = SessionHelper.getWhiteboardFieldsNum(wbModel.getWbId(),
                    session);
                final long completeFieldsNum = SessionHelper.getNumEntriesWithStateForWhiteboard(
                    wbModel.getWbId(), State.FINISHED, session);
                if (fieldsNum == completeFieldsNum) {
                    wbModel.setWbState(WhiteboardStatus.State.COMPLETED);
                } else {
                    wbModel.setWbState(WhiteboardStatus.State.NOT_COMPLETED);
                }
            }

            try {
                session.update(snapshotModel);
                snapshotEntries.forEach(session::update);
                whiteboards.forEach(session::update);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void error(Snapshot snapshot) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String snapshotId = snapshot.id().toString();
            SnapshotModel snapshotModel = session.find(SnapshotModel.class, snapshotId);
            if (snapshotModel == null) {
                throw new RuntimeException(Status.NOT_FOUND.asException());
            }
            snapshotModel.setSnapshotState(SnapshotStatus.State.ERRORED);

            List<WhiteboardModel> resultList = SessionHelper.getWhiteboardModels(snapshotId,
                session);
            resultList.forEach(wb -> wb.setWbState(WhiteboardStatus.State.ERRORED));

            try {
                session.update(snapshotModel);
                for (WhiteboardModel wbModel : resultList) {
                    session.update(wbModel);
                }
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void prepare(SnapshotEntry entry, String storageUri, List<String> dependentEntryIds) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String snapshotId = entry.snapshot().id().toString();
            String entryId = entry.id();
            SnapshotEntryModel snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, entryId));
            if (snapshotEntryModel == null) {
                snapshotEntryModel = new SnapshotEntryModel(snapshotId, entryId,
                    storageUri, true, SnapshotEntryStatus.State.IN_PROGRESS);
            } else {
                if (!snapshotEntryModel.isEmpty()) {
                    throw Status.INVALID_ARGUMENT.withDescription("Preparing non-empty entry")
                        .asRuntimeException();
                }
                if (!snapshotEntryModel.getEntryState().equals(SnapshotEntryStatus.State.CREATED)) {
                    throw Status.INVALID_ARGUMENT.withDescription(
                        "Preparing already prepared entry").asRuntimeException();
                }
                snapshotEntryModel.setStorageUri(storageUri);
                snapshotEntryModel.setEntryState(SnapshotEntryStatus.State.IN_PROGRESS);
                snapshotEntryModel.setEmpty(true);
            }

            List<EntryDependenciesModel> depModelList = new ArrayList<>();
            dependentEntryIds.forEach(
                id -> depModelList.add(new EntryDependenciesModel(snapshotId, id, entryId)));
            try {
                session.save(snapshotEntryModel);
                depModelList.forEach(session::save);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Nullable
    @Override
    public SnapshotEntry resolveEntry(Snapshot snapshot, String id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            String snapshotId = snapshot.id().toString();
            SnapshotEntryModel snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, id));
            if (snapshotEntryModel == null) {
                return null;
            }
            return new SnapshotEntry.Impl(id, snapshot);
        }
    }

    @Nullable
    @Override
    public SnapshotEntryStatus resolveEntryStatus(Snapshot snapshot, String id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            return SessionHelper.resolveEntryStatus(snapshot, id, session);
        }
    }

    @Nullable
    @Override
    public SnapshotStatus lastSnapshot(String workflowName, String uid) {
        try (Session session = storage.getSessionFactory().openSession()) {
            return SessionHelper.lastSnapshot(workflowName, uid, session);
        }
    }

    @Override
    public void commit(SnapshotEntry entry, boolean empty) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String snapshotId = entry.snapshot().id().toString();
            String entryId = entry.id();
            SnapshotEntryModel snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, entryId));
            if (snapshotEntryModel == null) {
                throw new RuntimeException(Status.NOT_FOUND.asException());
            }
            snapshotEntryModel.setEntryState(SnapshotEntryStatus.State.FINISHED);
            snapshotEntryModel.setEmpty(empty);
            try {
                session.update(snapshotEntryModel);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SnapshotEntryStatus createEntry(Snapshot snapshot, String id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                SnapshotEntryModel model = session.find(SnapshotEntryModel.class,
                    new SnapshotEntryModel.SnapshotEntryPk(snapshot.id().toString(), id));
                if (model != null) {
                    throw Status.ALREADY_EXISTS.withDescription("Creating existing entry")
                        .asRuntimeException();
                }
                model = new SnapshotEntryModel(snapshot.id().toString(), id, null, true,
                    SnapshotEntryStatus.State.CREATED);
                session.save(model);
                tx.commit();
                final Impl entry = new Impl(id, snapshot);
                return new SnapshotEntryStatus.Impl(model.isEmpty(), model.getEntryState(), entry,
                    Collections.emptySet(), null);
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }
}
