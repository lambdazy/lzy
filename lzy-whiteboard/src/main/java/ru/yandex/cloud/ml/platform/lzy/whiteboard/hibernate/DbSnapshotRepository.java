package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate;

import io.grpc.Status;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
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
    public @NotNull SnapshotStatus create(@NotNull Snapshot snapshot) throws IllegalArgumentException {
        try (Session session = storage.getSessionFactory().openSession()) {
            String snapshotId = snapshot.id().toString();
            SnapshotModel snapshotStatus = session.find(SnapshotModel.class, snapshotId);
            if (snapshotStatus != null) {
                LOG.error("Snapshot with id {} already exists", snapshotId);
                throw new IllegalArgumentException("Snapshot with id " + snapshot.id() + " already exists");
            }

            Transaction tx = session.beginTransaction();
            snapshotStatus = new SnapshotModel(snapshotId, SnapshotStatus.State.CREATED,
                snapshot.uid().toString(), snapshot.creationDateUTC(), snapshot.workflowName(),
                snapshot.parentSnapshotId());
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
    public @NotNull SnapshotStatus createFromSnapshot(@NotNull String fromSnapshotId, @NotNull Snapshot snapshot)
        throws IllegalArgumentException {
        try (Session session = storage.getSessionFactory().openSession()) {
            SnapshotModel fromSnapshotModel = session.find(SnapshotModel.class, fromSnapshotId);
            if (fromSnapshotModel == null) {
                throw new IllegalArgumentException("Snapshot with id "
                    + fromSnapshotId + " does not exists; snapshot with id "
                    + snapshot.id().toString() + " could not be created"
                );
            }

            String snapshotId = snapshot.id().toString();
            SnapshotModel snapshotModel = session.find(SnapshotModel.class, snapshotId);
            if (snapshotModel != null) {
                throw new IllegalArgumentException("Snapshot with id " + snapshotId + " already exists");
            }

            Transaction tx = session.beginTransaction();
            snapshotModel = new SnapshotModel(snapshotId, SnapshotStatus.State.CREATED,
                snapshot.uid().toString(), snapshot.creationDateUTC(), snapshot.workflowName(), fromSnapshotId);
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

    @Override
    public Optional<SnapshotStatus> resolveSnapshot(@NotNull URI id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            SnapshotModel snapshotModel = session.find(SnapshotModel.class, id.toString());
            if (snapshotModel == null) {
                LOG.info("Snapshot with id {} not found", id);
                return Optional.empty();
            }
            LOG.info("Found snapshot with id {}: {}", id, snapshotModel);
            return Optional.of(new SnapshotStatus.Impl(
                new Snapshot.Impl(
                    id,
                    URI.create(snapshotModel.getUid()),
                    snapshotModel.creationDateUTC(),
                    snapshotModel.workflowName(),
                    snapshotModel.parentSnapshotId()
                ),
                snapshotModel.getSnapshotState()));
        }
    }

    @Override
    public void finalize(@NotNull Snapshot snapshot) throws IllegalArgumentException {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String snapshotId = snapshot.id().toString();
            SnapshotModel snapshotModel = session.find(SnapshotModel.class, snapshotId);
            if (snapshotModel == null) {
                throw new IllegalArgumentException(Status.NOT_FOUND.withDescription(
                    "Could not find snapshot " + snapshot).asException());
            }
            if (snapshotModel.getSnapshotState() == SnapshotStatus.State.ERRORED) {
                LOG.warn("Could not finalize snapshot {} because its status is {} ", snapshot,
                    SnapshotStatus.State.ERRORED);
                return;
            }
            if (snapshotModel.getSnapshotState() == SnapshotStatus.State.FINALIZED) {
                LOG.warn("Could not finalize snapshot {} because its status is {} ", snapshot,
                    SnapshotStatus.State.FINALIZED);
                return;
            }
            List<SnapshotEntryModel> snapshotEntries = SessionHelper.getSnapshotEntries(snapshotId, session);
            for (SnapshotEntryModel spEntry : snapshotEntries) {
                if (spEntry.getEntryState() != State.FINISHED || spEntry.getStorageUri() == null) {
                    LOG.warn("Error in entry {}: status {}", spEntry.getEntryId(),
                        spEntry.getEntryState());
                    spEntry.setEntryState(State.ERRORED);
                    snapshotModel.setSnapshotState(SnapshotStatus.State.ERRORED);
                }
            }
            if (snapshotModel.getSnapshotState() != SnapshotStatus.State.ERRORED) {
                LOG.info("Finalized snapshot with id {}", snapshot.id());
                snapshotModel.setSnapshotState(SnapshotStatus.State.FINALIZED);
            }

            List<WhiteboardModel> whiteboards = SessionHelper.getWhiteboardModels(snapshotId,
                session);
            for (WhiteboardModel wbModel : whiteboards) {
                if (snapshotModel.getSnapshotState() == SnapshotStatus.State.ERRORED) {
                    LOG.info("Setting state for whiteboard {} to {}", wbModel, WhiteboardStatus.State.ERRORED);
                    wbModel.setWbState(WhiteboardStatus.State.ERRORED);
                    continue;
                }
                final long fieldsNum = SessionHelper.getWhiteboardFieldsNum(wbModel.getWbId(),
                    session);
                final long completeFieldsNum = SessionHelper.getNumEntriesWithStateForWhiteboard(
                    wbModel.getWbId(), State.FINISHED, session);
                LOG.info("Whiteboard {} has {} in total and {} with status {}", wbModel, fieldsNum, completeFieldsNum,
                    State.FINISHED);
                if (fieldsNum == completeFieldsNum) {
                    LOG.info("Setting state for whiteboard {} to {}", wbModel, WhiteboardStatus.State.COMPLETED);
                    wbModel.setWbState(WhiteboardStatus.State.COMPLETED);
                } else {
                    LOG.info("Setting state for whiteboard {} to {}", wbModel, WhiteboardStatus.State.ERRORED);
                    wbModel.setWbState(WhiteboardStatus.State.ERRORED);
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
    public void error(@NotNull Snapshot snapshot) throws IllegalArgumentException {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String snapshotId = snapshot.id().toString();
            SnapshotModel snapshotModel = session.find(SnapshotModel.class, snapshotId);
            if (snapshotModel == null) {
                throw new IllegalArgumentException(
                    Status.NOT_FOUND.withDescription("Snapshot with id " + snapshotId + " not found").asException());
            }
            snapshotModel.setSnapshotState(SnapshotStatus.State.ERRORED);

            List<WhiteboardModel> resultList = SessionHelper.getWhiteboardModels(snapshotId,
                session);
            resultList.forEach(wb -> wb.setWbState(WhiteboardStatus.State.ERRORED));

            try {
                session.update(snapshotModel);
                LOG.info("Set snapshot {} state to {}", snapshotId, SnapshotStatus.State.ERRORED);
                for (WhiteboardModel wbModel : resultList) {
                    session.update(wbModel);
                    LOG.info("Set whiteboard {} state to {}", wbModel.getWbId(), WhiteboardStatus.State.ERRORED);
                }
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void prepare(@NotNull SnapshotEntry entry, @NotNull String storageUri,
        @NotNull List<String> dependentEntryIds)
        throws IllegalArgumentException {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String snapshotId = entry.snapshot().id().toString();
            String entryId = entry.id();
            SnapshotEntryModel snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, entryId));
            if (snapshotEntryModel == null) {
                throw new IllegalArgumentException(
                    "Could not execute command prepare in DbSnapshotRepository because snapshot entry with id "
                        + entryId + " and snapshot id " + snapshotId + " was not found");
            } else {
                if (!snapshotEntryModel.getEntryState().equals(SnapshotEntryStatus.State.CREATED)) {
                    throw new IllegalArgumentException(
                        "Could not execute command prepare in DbSnapshotRepository because snapshot entry with id "
                            + entryId + " and snapshot id " + snapshotId + " has status "
                            + snapshotEntryModel.getEntryState() + "; but status " + SnapshotEntryStatus.State.CREATED
                            + " was expected");
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

    @Override
    public Optional<SnapshotEntry> resolveEntry(@NotNull Snapshot snapshot, @NotNull String id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            String snapshotId = snapshot.id().toString();
            SnapshotEntryModel snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, id));
            if (snapshotEntryModel == null) {
                LOG.info(
                    "DbSnapshotRepository::resolveEntry snapshot entry with id " + id + " and snapshot id " + snapshotId
                        + " not found");
                return Optional.empty();
            }
            return Optional.of(new SnapshotEntry.Impl(id, snapshot));
        }
    }

    @Override
    public Optional<SnapshotEntryStatus> resolveEntryStatus(@NotNull Snapshot snapshot, @NotNull String id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            return SessionHelper.resolveEntryStatus(snapshot, id, session);
        }
    }

    @Override
    public Optional<SnapshotStatus> lastSnapshot(@NotNull String workflowName, @NotNull String uid) {
        try (Session session = storage.getSessionFactory().openSession()) {
            return SessionHelper.lastSnapshot(workflowName, uid, session);
        }
    }

    @Override
    public void commit(@NotNull SnapshotEntry entry, boolean empty) throws IllegalArgumentException {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String snapshotId = entry.snapshot().id().toString();
            String entryId = entry.id();
            SnapshotEntryModel snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, entryId));
            if (snapshotEntryModel == null) {
                throw new IllegalArgumentException(Status.NOT_FOUND.withDescription(
                    "DbSnapshotRepository::commit snapshot entry " + entry + " not found").asException());
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
    public @NotNull SnapshotEntry createEntry(@NotNull Snapshot snapshot, @NotNull String id)
        throws IllegalArgumentException {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                SnapshotEntryModel model = session.find(SnapshotEntryModel.class,
                    new SnapshotEntryModel.SnapshotEntryPk(snapshot.id().toString(), id));
                if (model != null) {
                    throw new IllegalArgumentException(
                        "DbSnapshotRepository::createEntry entry with id " + id + " and snapshot id " + snapshot.id()
                            + " already exists");
                }
                model = new SnapshotEntryModel(snapshot.id().toString(), id, null, true,
                    SnapshotEntryStatus.State.CREATED);
                session.save(model);
                tx.commit();
                return new Impl(id, snapshot);
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }
}
