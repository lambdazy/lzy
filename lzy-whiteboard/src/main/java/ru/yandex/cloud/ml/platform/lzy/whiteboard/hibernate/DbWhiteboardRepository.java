package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.Impl;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions.WhiteboardRepositoryException;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardFieldModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardTagModel;

@Singleton
@Requires(beans = DbStorage.class)
public class DbWhiteboardRepository implements WhiteboardRepository {

    private static final Logger LOG = LogManager.getLogger(DbWhiteboardRepository.class);

    @Inject
    DbStorage storage;

    @Override
    public @NotNull WhiteboardStatus create(@NotNull Whiteboard whiteboard) throws WhiteboardRepositoryException {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String wbId = whiteboard.id().toString();
            WhiteboardModel wbModel = session.find(WhiteboardModel.class, wbId);
            if (wbModel != null) {
                LOG.error("DbWhiteboardRepository::create whiteboard with id " + wbId + " already exists");
                throw new WhiteboardRepositoryException("Whiteboard with id " + wbId + " already exists");
            }
            wbModel = new WhiteboardModel(
                wbId, WhiteboardStatus.State.CREATED, whiteboard.snapshot().id().toString(),
                whiteboard.namespace(), whiteboard.creationDateUTC()
            );
            List<WhiteboardFieldModel> whiteboardFieldModels = whiteboard.fieldNames().stream()
                .map(fieldName -> new WhiteboardFieldModel(wbId, fieldName, null))
                .collect(Collectors.toList());
            List<WhiteboardTagModel> whiteboardTagModels = whiteboard.tags().stream()
                .map(tag -> new WhiteboardTagModel(wbId, tag))
                .collect(Collectors.toList());
            try {
                session.save(wbModel);
                whiteboardFieldModels.forEach(session::save);
                whiteboardTagModels.forEach(session::save);
                tx.commit();
                return new Impl(whiteboard, wbModel.getWbState());
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Optional<WhiteboardStatus> resolveWhiteboard(@NotNull URI id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Whiteboard whiteboard = SessionHelper.resolveWhiteboard(id.toString(), session);
            if (whiteboard == null) {
                return Optional.empty();
            }
            return Optional.of(
                new WhiteboardStatus.Impl(whiteboard, SessionHelper.resolveWhiteboardState(id.toString(), session)));
        }
    }

    @Override
    public Stream<WhiteboardStatus> resolveWhiteboards(String namespace, List<String> tags,
        Date fromDateUTCIncluded, Date toDateUTCExcluded) {
        List<String> ids;
        try (Session session = storage.getSessionFactory().openSession()) {
            ids = SessionHelper.resolveWhiteboardIds(namespace, tags, fromDateUTCIncluded, toDateUTCExcluded, session);
        }
        return ids.stream().map(id -> {
            Optional<WhiteboardStatus> whiteboardStatusOptional = resolveWhiteboard(URI.create(id));
            if (whiteboardStatusOptional.isEmpty()) {
                throw new IllegalArgumentException("Could not resolve whiteboard with id " + id);
            }
            return whiteboardStatusOptional.get();
        });
    }

    @Override
    public void update(WhiteboardField field) throws WhiteboardRepositoryException {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String wbFieldId = field.whiteboard().id().toString();
            WhiteboardFieldModel wbModel = session.find(WhiteboardFieldModel.class,
                new WhiteboardFieldModel.WhiteboardFieldPk(wbFieldId, field.name()));
            if (wbModel == null) {
                throw new WhiteboardRepositoryException("Could not find whiteboard field with id " + wbFieldId);
            }
            final SnapshotEntry entry = field.entry();
            if (entry != null) {
                wbModel.setEntryId(entry.id());
            }
            try {
                session.update(wbModel);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Stream<WhiteboardField> dependent(WhiteboardField field) {
        try (Session session = storage.getSessionFactory().openSession()) {
            String snapshotId = field.whiteboard().snapshot().id().toString();
            Snapshot snapshot = SessionHelper.getSnapshot(snapshotId, session);
            if (snapshot == null) {
                throw new IllegalArgumentException(
                    "Could not resolve snapshot with id " + snapshotId + " for whiteboard field " + field);
            }
            String whiteboardId = field.whiteboard().id().toString();
            Whiteboard whiteboard = SessionHelper.getWhiteboard(whiteboardId, snapshot, session);
            if (whiteboard == null) {
                throw new IllegalArgumentException(
                    "Could not resolve whiteboard with id " + whiteboardId + " for whiteboard field " + field);
            }
            List<WhiteboardFieldModel> wbFieldModelList =
                SessionHelper.getFieldDependencies(field.whiteboard().id().toString(), field.name(), session);
            List<WhiteboardField> result = wbFieldModelList.stream()
                .map(w -> SessionHelper.getWhiteboardField(w, whiteboard, snapshot, session))
                .collect(Collectors.toList());
            return result.stream();
        }
    }

    @Override
    public Stream<WhiteboardField> fields(Whiteboard whiteboard) throws WhiteboardRepositoryException {
        try (Session session = storage.getSessionFactory().openSession()) {
            String snapshotId = whiteboard.snapshot().id().toString();
            Snapshot snapshot = SessionHelper.getSnapshot(snapshotId, session);
            if (snapshot == null) {
                throw new WhiteboardRepositoryException(
                    "Could not resolve snapshot with id " + snapshotId + " for whiteboard with id " + whiteboard.id());
            }
            List<WhiteboardFieldModel> wbFieldModelList =
                SessionHelper.getWhiteboardFields(whiteboard.id().toString(), session);
            List<WhiteboardField> result = wbFieldModelList.stream()
                .map(w -> SessionHelper.getWhiteboardField(w, whiteboard, snapshot, session))
                .collect(Collectors.toList());
            return result.stream();
        }
    }
}
