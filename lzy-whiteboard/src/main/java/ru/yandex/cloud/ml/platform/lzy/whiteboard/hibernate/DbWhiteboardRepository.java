package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate;

import io.grpc.Status;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.Impl;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardFieldModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardTagModel;

@Singleton
@Requires(beans = DbStorage.class)
public class DbWhiteboardRepository implements WhiteboardRepository {
    @Inject
    DbStorage storage;

    @Override
    public WhiteboardStatus create(Whiteboard whiteboard) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            String wbId = whiteboard.id().toString();
            WhiteboardModel wbModel = new WhiteboardModel(wbId, WhiteboardStatus.State.CREATED,
                    whiteboard.snapshot().id().toString(), whiteboard.namespace());
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

    @Nullable
    @Override
    public WhiteboardStatus resolveWhiteboard(URI id) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Whiteboard whiteboard = SessionHelper.resolveWhiteboard(id.toString(), session);
            if (whiteboard == null) {
                return null;
            }
            return new WhiteboardStatus.Impl(whiteboard, SessionHelper.resolveWhiteboardState(id.toString(), session));
        }
    }

    @Override
    public List<WhiteboardStatus> resolveWhiteboards(String namespace, List<String> tags) {
        List<String> ids;
        try (Session session = storage.getSessionFactory().openSession()) {
            ids = SessionHelper.getWhiteboardIdByNamespaceAndTags(namespace, tags, session);
        }
        return ids.stream().map(id -> resolveWhiteboard(URI.create(id))).collect(Collectors.toList());
    }

    @Override
    public void update(WhiteboardField field) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            WhiteboardFieldModel wbModel = session.find(WhiteboardFieldModel.class,
                    new WhiteboardFieldModel.WhiteboardFieldPk(field.whiteboard().id().toString(), field.name()));
            if (wbModel == null) {
                throw new RuntimeException(Status.NOT_FOUND.asException());
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
            Snapshot snapshot = SessionHelper.getSnapshot(field.whiteboard().snapshot().id().toString(), session);
            Whiteboard whiteboard = SessionHelper.getWhiteboard(field.whiteboard().id().toString(), snapshot, session);
            List<WhiteboardFieldModel> wbFieldModelList = SessionHelper.getFieldDependencies(field.whiteboard().id().toString(), field.name(), session);
            List<WhiteboardField> result = wbFieldModelList.stream()
                    .map(w -> SessionHelper.getWhiteboardField(w, whiteboard, snapshot, session))
                    .collect(Collectors.toList());
            return result.stream();
        }
    }

    @Override
    public Stream<WhiteboardField> fields(Whiteboard whiteboard) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Snapshot snapshot = SessionHelper.getSnapshot(whiteboard.snapshot().id().toString(), session);
            List<WhiteboardFieldModel> wbFieldModelList = SessionHelper.getWhiteboardFields(whiteboard.id().toString(), session);
            List<WhiteboardField> result = wbFieldModelList.stream()
                    .map(w -> SessionHelper.getWhiteboardField(w, whiteboard, snapshot, session))
                    .collect(Collectors.toList());
            return result.stream();
        }
    }
}
