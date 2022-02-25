package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus.State.FINISHED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus.State;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus.State.FINALIZED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.COMPLETED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.CREATED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.ERRORED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.NOT_COMPLETED;

import io.micronaut.context.ApplicationContext;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbSnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.EntryDependenciesModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotEntryModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardFieldModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardModel;

public class DbSnapshotRepositoryTest {

    private final String storageUri = "storageUri";
    private final String namespace = "namespace";
    private final Date creationDateUTC = Date.from(Instant.now());
    private final String workflowName = "workflow";
    DbSnapshotRepository impl;
    private ApplicationContext ctx;
    private DbStorage storage;
    private String snapshotId;
    private String wbIdFirst;
    private String wbIdSecond;
    private String entryIdFirst;
    private String entryIdSecond;
    private String entryIdThird;
    private URI snapshotOwner;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        impl = ctx.getBean(DbSnapshotRepository.class);
        storage = ctx.getBean(DbStorage.class);

        snapshotId = UUID.randomUUID().toString();
        wbIdFirst = UUID.randomUUID().toString();
        wbIdSecond = UUID.randomUUID().toString();
        entryIdFirst = UUID.randomUUID().toString();
        entryIdSecond = UUID.randomUUID().toString();
        entryIdThird = UUID.randomUUID().toString();
        snapshotOwner = URI.create(UUID.randomUUID().toString());
    }

    @After
    public void tearDown() {
        ctx.stop();
    }

    @Test
    public void testCreate() {
        SnapshotModel snapshotModel;
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName);
        impl.create(snapshot);
        try (Session session = storage.getSessionFactory().openSession()) {
            snapshotModel = session.find(SnapshotModel.class, snapshotId);
        }
        Assert.assertNotNull(snapshotModel);
        Assert.assertEquals(State.CREATED, snapshotModel.getSnapshotState());
        Assert.assertEquals(snapshotOwner.toString(), snapshotModel.getUid());
    }

    @Test
    public void testResolveSnapshotNotNull() {
        impl.create(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName));

        SnapshotStatus snapshotStatus = impl.resolveSnapshot(URI.create(snapshotId));

        Assert.assertNotNull(snapshotStatus);
        Assert.assertEquals(State.CREATED, snapshotStatus.state());
    }

    @Test
    public void testResolveSnapshotNull() {
        SnapshotStatus snapshotStatus = impl.resolveSnapshot(URI.create(UUID.randomUUID().toString()));
        Assert.assertNull(snapshotStatus);
    }

    @Test
    public void testFinalizeSnapshotNotFound() {
        Assert.assertThrows(RuntimeException.class,
            () -> impl.finalize(
                new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName)));
    }

    @Test
    public void testFinalizeSnapshotErroredEntries() {
        try (Session session = storage.getSessionFactory().openSession()) {
            final Transaction tx = session.beginTransaction();
            session.save(
                new SnapshotModel(snapshotId, State.CREATED, snapshotOwner.toString(), creationDateUTC, workflowName));
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, storageUri, false, FINISHED));
            session.save(new SnapshotEntryModel(snapshotId, entryIdSecond, storageUri, false,
                SnapshotEntryStatus.State.CREATED));
            tx.commit();
        }
        impl.finalize(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName));
        SnapshotStatus status = impl.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(status);
        Assert.assertEquals(SnapshotStatus.State.ERRORED, status.state());
    }

    @Test
    public void testFinalizeSnapshotOkEntries() {
        try (Session session = storage.getSessionFactory().openSession()) {
            final Transaction tx = session.beginTransaction();
            session.save(
                new SnapshotModel(snapshotId, State.CREATED, snapshotOwner.toString(), creationDateUTC, workflowName));
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, storageUri, false, FINISHED));
            session.save(new SnapshotEntryModel(snapshotId, entryIdSecond, storageUri, false, FINISHED));
            tx.commit();
        }
        impl.finalize(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName));
        SnapshotStatus status = impl.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(status);
        Assert.assertEquals(FINALIZED, status.state());
    }

    @Test
    public void testFinalizeSnapshotNullStorageUriEntries() {
        try (Session session = storage.getSessionFactory().openSession()) {
            final Transaction tx = session.beginTransaction();
            session.save(
                new SnapshotModel(snapshotId, State.CREATED, snapshotOwner.toString(), creationDateUTC, workflowName));
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, null, false, FINISHED));
            session.save(new SnapshotEntryModel(snapshotId, entryIdSecond, storageUri, false, FINISHED));
            tx.commit();
        }
        impl.finalize(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName));
        SnapshotStatus status = impl.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(status);
        Assert.assertEquals(SnapshotStatus.State.ERRORED, status.state());
    }

    @Test
    public void testFinalizeSnapshot() {
        try (Session session = storage.getSessionFactory().openSession()) {
            final Transaction tx = session.beginTransaction();
            session.save(
                new SnapshotModel(snapshotId, State.CREATED, snapshotOwner.toString(), creationDateUTC, workflowName));
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotId, namespace, creationDateUTC));
            session.save(new WhiteboardModel(wbIdSecond, CREATED, snapshotId, namespace, creationDateUTC));
            String fieldNameFirst = "fieldNameFirst";
            final String entryIdFirst = UUID.randomUUID().toString();
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFirst, entryIdFirst));
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, storageUri, false, FINISHED));
            String fieldNameSecond = "fieldNameSecond";
            final String entryIdSecond = UUID.randomUUID().toString();
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameSecond, null));
            session.save(new SnapshotEntryModel(snapshotId, entryIdSecond, storageUri, false, FINISHED));
            String fieldNameThird = "fieldNameThird";
            String entryIdThird = UUID.randomUUID().toString();
            session.save(new WhiteboardFieldModel(wbIdSecond, fieldNameThird, entryIdThird));
            session.save(new SnapshotEntryModel(snapshotId, entryIdThird, storageUri, false, FINISHED));
            String fieldNameFourth = "fieldNameFourth";
            final String entryIdFourth = UUID.randomUUID().toString();
            session.save(new WhiteboardFieldModel(wbIdSecond, fieldNameFourth, entryIdFourth));
            session.save(new SnapshotEntryModel(snapshotId, entryIdFourth, storageUri, false, FINISHED));
            tx.commit();
        }
        impl.finalize(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName));
        SnapshotModel snapshotModel;
        WhiteboardModel whiteboardModelFirst;
        WhiteboardModel whiteboardModelSecond;
        try (Session session = storage.getSessionFactory().openSession()) {
            snapshotModel = session.find(SnapshotModel.class, snapshotId);
            whiteboardModelFirst = session.find(WhiteboardModel.class, wbIdFirst);
            whiteboardModelSecond = session.find(WhiteboardModel.class, wbIdSecond);
        }
        Assert.assertEquals(FINALIZED, snapshotModel.getSnapshotState());
        Assert.assertEquals(NOT_COMPLETED, whiteboardModelFirst.getWbState());
        Assert.assertEquals(COMPLETED, whiteboardModelSecond.getWbState());
    }

    @Test
    public void testErrorSnapshotNotFound() {
        Assert.assertThrows(RuntimeException.class,
            () -> impl.error(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName)));
    }

    @Test
    public void testErrorSnapshot() {
        try (Session session = storage.getSessionFactory().openSession()) {
            final Transaction tx = session.beginTransaction();
            session.save(
                new SnapshotModel(snapshotId, State.CREATED, snapshotOwner.toString(), creationDateUTC, workflowName));
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotId, namespace, creationDateUTC));
            session.save(new WhiteboardModel(wbIdSecond, CREATED, snapshotId, namespace, creationDateUTC));
            tx.commit();
        }
        impl.error(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName));
        SnapshotModel snapshotModel;
        WhiteboardModel whiteboardModelFirst;
        WhiteboardModel whiteboardModelSecond;
        try (Session session = storage.getSessionFactory().openSession()) {
            snapshotModel = session.find(SnapshotModel.class, snapshotId);
            whiteboardModelFirst = session.find(WhiteboardModel.class, wbIdFirst);
            whiteboardModelSecond = session.find(WhiteboardModel.class, wbIdSecond);
        }
        Assert.assertEquals(State.ERRORED, snapshotModel.getSnapshotState());
        Assert.assertEquals(ERRORED, whiteboardModelFirst.getWbState());
        Assert.assertEquals(ERRORED, whiteboardModelSecond.getWbState());
    }

    @Test
    public void testPrepareEntryExists() {
        try (Session session = storage.getSessionFactory().openSession()) {
            final Transaction tx = session.beginTransaction();
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, storageUri,
                false, FINISHED));
            tx.commit();
        }
        SnapshotEntry snapshotEntry =
            new SnapshotEntry.Impl(entryIdFirst,
                new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName));
        Assert.assertThrows(RuntimeException.class,
            () -> impl.prepare(snapshotEntry, storageUri, Collections.emptyList()));
    }

    @Test
    public void testPrepareEntry() {
        SnapshotEntry snapshotEntry =
            new SnapshotEntry.Impl(entryIdFirst,
                new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName));
        impl.prepare(snapshotEntry, storageUri, List.of(entryIdSecond, entryIdThird));
        SnapshotEntryModel snapshotEntryModel;
        EntryDependenciesModel entryDependenciesFirst;
        EntryDependenciesModel entryDependenciesSecond;
        try (Session session = storage.getSessionFactory().openSession()) {
            snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, entryIdFirst));
            entryDependenciesFirst = session.find(EntryDependenciesModel.class,
                new EntryDependenciesModel.EntryDependenciesPk(snapshotId, entryIdSecond, entryIdFirst));
            entryDependenciesSecond = session.find(EntryDependenciesModel.class,
                new EntryDependenciesModel.EntryDependenciesPk(snapshotId, entryIdThird, entryIdFirst));
        }
        Assert.assertNotNull(snapshotEntryModel);
        Assert.assertEquals(storageUri, snapshotEntryModel.getStorageUri());
        Assert.assertTrue(snapshotEntryModel.isEmpty());
        Assert.assertEquals(SnapshotEntryStatus.State.IN_PROGRESS, snapshotEntryModel.getEntryState());
        Assert.assertNotNull(entryDependenciesFirst);
        Assert.assertNotNull(entryDependenciesSecond);

    }

    @Test
    public void testResolveEntryNotFound() {
        Assert.assertNull(
            impl.resolveEntry(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName),
                entryIdFirst));
    }

    @Test
    public void testResolveEntryStatus() {
        try (Session session = storage.getSessionFactory().openSession()) {
            final Transaction tx = session.beginTransaction();
            session.save(
                new SnapshotModel(snapshotId, State.CREATED, snapshotOwner.toString(), creationDateUTC, workflowName));
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, storageUri,
                false, FINISHED));
            session.save(new SnapshotEntryModel(UUID.randomUUID().toString(), entryIdSecond, storageUri,
                false, FINISHED));
            session.save(new SnapshotEntryModel(UUID.randomUUID().toString(), entryIdThird, storageUri,
                false, FINISHED));
            session.save(new EntryDependenciesModel(snapshotId, entryIdSecond, entryIdFirst));
            session.save(new EntryDependenciesModel(snapshotId, entryIdThird, entryIdFirst));
            tx.commit();
        }
        SnapshotEntryStatus snapshotEntryStatus = impl.resolveEntryStatus(
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName), entryIdFirst
        );
        Assert.assertNotNull(snapshotEntryStatus);
        Assert.assertEquals(snapshotId, snapshotEntryStatus.entry().snapshot().id().toString());
        Assert.assertEquals(entryIdFirst, snapshotEntryStatus.entry().id());
        Assert.assertNotNull(snapshotEntryStatus.storage());
        Assert.assertEquals(storageUri, snapshotEntryStatus.storage().toString());
        Assert.assertTrue(snapshotEntryStatus.dependentEntryIds().contains(entryIdSecond)
            && snapshotEntryStatus.dependentEntryIds().contains(entryIdThird));
    }

    @Test
    public void testCommitNotFound() {
        Assert.assertThrows(RuntimeException.class,
            () -> impl.commit(new SnapshotEntry.Impl(UUID.randomUUID().toString(),
                new Snapshot.Impl(URI.create(UUID.randomUUID().toString()), snapshotOwner, creationDateUTC,
                    workflowName)), true)
        );
    }

    @Test
    public void testCommit() {
        try (Session session = storage.getSessionFactory().openSession()) {
            final Transaction tx = session.beginTransaction();
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, storageUri,
                true, SnapshotEntryStatus.State.IN_PROGRESS));
            tx.commit();
        }
        impl.commit(new SnapshotEntry.Impl(entryIdFirst,
                new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName)),
            false);
        SnapshotEntryModel snapshotEntryModel;
        try (Session session = storage.getSessionFactory().openSession()) {
            snapshotEntryModel = session.find(SnapshotEntryModel.class,
                new SnapshotEntryModel.SnapshotEntryPk(snapshotId, entryIdFirst));
        }
        Assert.assertEquals(FINISHED, snapshotEntryModel.getEntryState());
        Assert.assertFalse(snapshotEntryModel.isEmpty());
    }
}
