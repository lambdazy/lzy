package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.micronaut.context.ApplicationContext;
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
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exception.SnapshotException;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.mem.SnapshotRepositoryImpl;


import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus.State.FINISHED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus.State;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus.State.FINALIZED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.*;

public class SnapshotRepositoryImplTest {
    private ApplicationContext ctx;
    SnapshotRepositoryImpl impl;
    private DbStorage storage;

    private String snapshotId;
    private String wbIdFirst;
    private String wbIdSecond;
    private String entryIdFirst;
    private String entryIdSecond;
    private String entryIdThird;
    private final String ownerId = "ownerId";
    private final String storageUri = "storageUri";

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        impl = ctx.getBean(SnapshotRepositoryImpl.class);
        storage = ctx.getBean(DbStorage.class);

        snapshotId = UUID.randomUUID().toString();
        wbIdFirst = UUID.randomUUID().toString();
        wbIdSecond = UUID.randomUUID().toString();
        entryIdFirst = UUID.randomUUID().toString();
        entryIdSecond = UUID.randomUUID().toString();
        entryIdThird = UUID.randomUUID().toString();
    }

    @After
    public void tearDown() {
        ctx.stop();
    }

    @Test
    public void testCreate(){
        SnapshotModel snapshotModel;
        SnapshotOwnerModel snapshotOwnerModel;
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId));
        impl.create(snapshot);
        try (Session session = storage.getSessionFactory().openSession()) {
            snapshotModel = session.find(SnapshotModel.class, snapshotId);
            snapshotOwnerModel = session.find(SnapshotOwnerModel.class, snapshotId);
        }
        Assert.assertNotNull(snapshotModel);
        Assert.assertEquals(State.CREATED, snapshotModel.getSnapshotState());
        Assert.assertNotNull(snapshotOwnerModel);
        Assert.assertEquals(ownerId, snapshotOwnerModel.getOwnerId());
    }

    @Test
    public void testResolveSnapshotNotNull() {
        impl.create(new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId)));

        SnapshotStatus snapshotStatus = impl.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(snapshotStatus);
        Assert.assertEquals(State.CREATED, snapshotStatus.state());
        Assert.assertEquals(ownerId, snapshotStatus.snapshot().ownerId().toString());
    }

    @Test
    public void testResolveSnapshotNull() {
        SnapshotStatus snapshotStatus = impl.resolveSnapshot(URI.create(UUID.randomUUID().toString()));
        Assert.assertNull(snapshotStatus);
    }

    @Test
    public void testFinalizeSnapshotNotFound() {
        Assert.assertThrows(SnapshotException.class, () -> impl.finalize(
                new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId))
            )
        );
    }

    @Test
    public void testFinalizeSnapshot() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new SnapshotModel(snapshotId, State.CREATED));
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotId));
            session.save(new WhiteboardModel(wbIdSecond, CREATED, snapshotId));
            String fieldNameFirst = "fieldNameFirst";
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFirst, null));
            String fieldNameSecond = "fieldNameSecond";
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameSecond, UUID.randomUUID().toString()));
            String fieldNameThird = "fieldNameThird";
            session.save(new WhiteboardFieldModel(wbIdSecond, fieldNameThird, UUID.randomUUID().toString()));
            String fieldNameFourth = "fieldNameFourth";
            session.save(new WhiteboardFieldModel(wbIdSecond, fieldNameFourth, UUID.randomUUID().toString()));
            tx.commit();
        }
        impl.finalize(new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId)));
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
        Assert.assertThrows(SnapshotException.class, () -> impl.error(
                new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId)))
        );
    }

    @Test
    public void testErrorSnapshot() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new SnapshotModel(snapshotId, State.CREATED));
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotId));
            session.save(new WhiteboardModel(wbIdSecond, CREATED, snapshotId));
            tx.commit();
        }
        impl.error(new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId)));
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
            Transaction tx = session.beginTransaction();
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, storageUri,
                    false, FINISHED));
            tx.commit();
        }
        SnapshotEntry snapshotEntry = new SnapshotEntry.Impl(entryIdFirst, URI.create(storageUri),
                Collections.emptySet(), new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId)), true);
        Assert.assertThrows(SnapshotException.class, () -> impl.prepare(snapshotEntry));
    }

    @Test
    public void testPrepareEntry() {
        SnapshotEntry snapshotEntry = new SnapshotEntry.Impl(entryIdFirst, URI.create(storageUri),
                Set.of(entryIdSecond, entryIdThird), new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId)), true);
        impl.prepare(snapshotEntry);
        SnapshotEntryModel snapshotEntryModel;
        EntryDependenciesModel entryDependenciesFirst;
        EntryDependenciesModel entryDependenciesSecond;
        try (Session session = storage.getSessionFactory().openSession()) {
            snapshotEntryModel = session.find(SnapshotEntryModel.class, new SnapshotEntryModel.SnapshotEntryPk(snapshotId, entryIdFirst));
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
        Assert.assertThrows(SnapshotException.class, () -> impl.resolveEntry(
                new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId)), entryIdFirst)
        );
    }

    @Test
    public void testResolveEntry() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new SnapshotModel(snapshotId, State.CREATED));
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
        SnapshotEntry snapshotEntry = impl.resolveEntry(new Snapshot.Impl(URI.create(snapshotId), URI.create(ownerId)), entryIdFirst);
        Assert.assertNotNull(snapshotEntry);
        Assert.assertEquals(snapshotId, snapshotEntry.snapshot().id().toString());
        Assert.assertEquals(entryIdFirst, snapshotEntry.id());
        Assert.assertEquals(storageUri, snapshotEntry.storage().toString());
        Assert.assertEquals(ownerId, snapshotEntry.snapshot().ownerId().toString());
        Assert.assertTrue(snapshotEntry.dependentEntryIds().contains(entryIdSecond)
                && snapshotEntry.dependentEntryIds().contains(entryIdThird));
    }

    @Test
    public void testCommitNotFound() {
        Assert.assertThrows(SnapshotException.class, () -> impl.commit(
                new SnapshotEntry.Impl(UUID.randomUUID().toString(), URI.create(UUID.randomUUID().toString()), Collections.emptySet(),
                new Snapshot.Impl(URI.create(UUID.randomUUID().toString()), URI.create(ownerId)), true)
        ));
    }

    @Test
    public void testCommit() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new SnapshotEntryModel(snapshotId, entryIdFirst, storageUri,
                    true, SnapshotEntryStatus.State.IN_PROGRESS));
            tx.commit();
        }
        impl.commit(new SnapshotEntry.Impl(entryIdFirst, URI.create(storageUri), Collections.emptySet(),
                new Snapshot.Impl(URI.create(snapshotId), URI.create(storageUri)), false));
        SnapshotEntryModel snapshotEntryModel;
        try (Session session = storage.getSessionFactory().openSession()) {
            snapshotEntryModel = session.find(SnapshotEntryModel.class,
                    new SnapshotEntryModel.SnapshotEntryPk(snapshotId, entryIdFirst));
        }
        Assert.assertEquals(FINISHED, snapshotEntryModel.getEntryState());
        Assert.assertFalse(snapshotEntryModel.isEmpty());
    }
}
