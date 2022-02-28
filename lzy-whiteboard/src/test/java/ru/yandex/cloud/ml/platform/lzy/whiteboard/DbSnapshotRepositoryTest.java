package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus.State.FINISHED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus.State;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus.State.FINALIZED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.CREATED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.ERRORED;

import io.micronaut.context.ApplicationContext;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
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
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard.Impl;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbSnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbWhiteboardRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.EntryDependenciesModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotEntryModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.SnapshotModel;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.WhiteboardModel;

public class DbSnapshotRepositoryTest {

    private final String storageUri = "storageUri";
    private final String namespace = "namespace";
    private final Date creationDateUTC = Date.from(Instant.now());
    private final String workflowName = "workflow";
    DbSnapshotRepository implSnapshotRepository;
    DbWhiteboardRepository implWhiteboardRepository;
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
        implSnapshotRepository = ctx.getBean(DbSnapshotRepository.class);
        implWhiteboardRepository = ctx.getBean(DbWhiteboardRepository.class);
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
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName);
        implSnapshotRepository.create(snapshot);
        SnapshotStatus snapshotResolved = implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(snapshotResolved);
        Assert.assertEquals(State.CREATED, snapshotResolved.state());
        Assert.assertEquals(snapshotOwner, snapshotResolved.snapshot().uid());
        Assert.assertEquals(creationDateUTC, snapshotResolved.snapshot().creationDateUTC());
        Assert.assertEquals(workflowName, snapshotResolved.snapshot().workflowName());
    }

    @Test
    public void testResolveSnapshotNull() {
        SnapshotStatus snapshotStatus = implSnapshotRepository.resolveSnapshot(URI.create(UUID.randomUUID().toString()));
        Assert.assertNull(snapshotStatus);
    }

    @Test
    public void testFinalizeSnapshotNotFound() {
        Assert.assertThrows(RuntimeException.class,
            () -> implSnapshotRepository.finalize(
                new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName)));
    }

    @Test
    public void testFinalizeSnapshotErroredEntries() {
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName);
        implSnapshotRepository.create(snapshot);
        implSnapshotRepository.prepare(new SnapshotEntry.Impl(entryIdFirst, snapshot), storageUri, Collections.emptyList());
        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshot);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);
        implSnapshotRepository.finalize(snapshot);
        SnapshotStatus status = implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(status);
        Assert.assertEquals(SnapshotStatus.State.ERRORED, status.state());
    }

    @Test
    public void testFinalizeSnapshotOkEntries() {
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName);
        implSnapshotRepository.create(snapshot);
        SnapshotEntry firstEntry = new SnapshotEntry.Impl(entryIdFirst, snapshot);
        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshot);
        implSnapshotRepository.prepare(firstEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(firstEntry, false);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);
        implSnapshotRepository.finalize(snapshot);
        SnapshotStatus status = implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(status);
        Assert.assertEquals(FINALIZED, status.state());
    }

    @Test
    public void testFinalizeSnapshotNullStorageUriEntries() {
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName);
        implSnapshotRepository.create(snapshot);
        SnapshotEntry firstEntry = new SnapshotEntry.Impl(entryIdFirst, snapshot);
        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshot);
        implSnapshotRepository.prepare(firstEntry, null, Collections.emptyList());
        implSnapshotRepository.commit(firstEntry, false);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);
        implSnapshotRepository.finalize(snapshot);
        SnapshotStatus status = implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(status);
        Assert.assertEquals(SnapshotStatus.State.ERRORED, status.state());
    }

    @Test
    public void testErrorSnapshotNotFound() {
        Assert.assertThrows(RuntimeException.class,
            () -> implSnapshotRepository.error(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName)));
    }

    @Test
    public void testErrorSnapshot() {
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName);
        implSnapshotRepository.create(snapshot);
        implWhiteboardRepository.create(
            new Impl(URI.create(wbIdFirst), Collections.emptySet(), snapshot, Collections.emptySet(), namespace, creationDateUTC)
        );
        implWhiteboardRepository.create(
            new Impl(URI.create(wbIdSecond), Collections.emptySet(), snapshot, Collections.emptySet(), namespace, creationDateUTC)
        );
        implSnapshotRepository.error(snapshot);
        WhiteboardStatus firstWhiteboard = implWhiteboardRepository.resolveWhiteboard(URI.create(wbIdFirst));
        WhiteboardStatus secondWhiteboard = implWhiteboardRepository.resolveWhiteboard(URI.create(wbIdSecond));
        SnapshotStatus snapshotStatus = implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertNotNull(snapshotStatus);
        Assert.assertEquals(State.ERRORED, snapshotStatus.state());
        Assert.assertNotNull(firstWhiteboard);
        Assert.assertEquals(ERRORED, firstWhiteboard.state());
        Assert.assertNotNull(secondWhiteboard);
        Assert.assertEquals(ERRORED, secondWhiteboard.state());
    }

    @Test
    public void testPrepareEntryExists() {
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName);
        implSnapshotRepository.create(snapshot);
        SnapshotEntry entry = new SnapshotEntry.Impl(entryIdFirst, snapshot);
        implSnapshotRepository.prepare(entry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(entry, false);
        Assert.assertThrows(RuntimeException.class,
            () -> implSnapshotRepository.prepare(entry, storageUri, Collections.emptyList()));
    }

    @Test
    public void testResolveEntryNotFound() {
        Assert.assertNull(
            implSnapshotRepository.resolveEntry(new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName),
                entryIdFirst));
    }

    @Test
    public void testResolveEntryStatus() {
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName);
        implSnapshotRepository.create(snapshot);
        SnapshotEntry firstEntry = new SnapshotEntry.Impl(entryIdFirst, snapshot);
        implSnapshotRepository.prepare(firstEntry, storageUri, List.of(entryIdSecond, entryIdThird));
        implSnapshotRepository.commit(firstEntry, false);

        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshot);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);

        SnapshotEntry thirdEntry = new SnapshotEntry.Impl(entryIdThird, snapshot);
        implSnapshotRepository.prepare(thirdEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(thirdEntry, false);

        SnapshotEntryStatus snapshotEntryStatus = implSnapshotRepository.resolveEntryStatus(
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName), entryIdFirst
        );
        Assert.assertNotNull(snapshotEntryStatus);
        Assert.assertEquals(snapshotId, snapshotEntryStatus.entry().snapshot().id().toString());
        Assert.assertEquals(entryIdFirst, snapshotEntryStatus.entry().id());
        Assert.assertNotNull(snapshotEntryStatus.storage());
        Assert.assertEquals(storageUri, Objects.requireNonNull(snapshotEntryStatus.storage()).toString());
        Assert.assertTrue(snapshotEntryStatus.dependentEntryIds().contains(entryIdSecond)
            && snapshotEntryStatus.dependentEntryIds().contains(entryIdThird));
    }

    @Test
    public void testCommitNotFound() {
        Assert.assertThrows(RuntimeException.class,
            () -> implSnapshotRepository.commit(new SnapshotEntry.Impl(UUID.randomUUID().toString(),
                new Snapshot.Impl(URI.create(UUID.randomUUID().toString()), snapshotOwner, creationDateUTC,
                    workflowName)), true)
        );
    }
}
