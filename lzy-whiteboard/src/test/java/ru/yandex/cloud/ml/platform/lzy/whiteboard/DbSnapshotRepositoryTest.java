package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus.State.FINISHED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus.State;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus.State.FINALIZED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.ERRORED;

import io.micronaut.context.ApplicationContext;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard.Impl;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions.SnapshotRepositoryException;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions.WhiteboardRepositoryException;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbSnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbWhiteboardRepository;

public class DbSnapshotRepositoryTest {

    private final String storageUri = "storageUri";
    private final Date creationDateUTC = Date.from(Instant.now());
    private final String workflowName = "workflow";
    DbSnapshotRepository implSnapshotRepository;
    DbWhiteboardRepository implWhiteboardRepository;
    private ApplicationContext ctx;
    private String snapshotId;
    private String wbIdFirst;
    private String wbIdSecond;
    private String entryIdFirst;
    private String entryIdSecond;
    private String entryIdThird;
    private URI snapshotOwner;

    private Date createDateUTC(int year, int month, int day, int hour, int minute) {
        return Date.from(LocalDateTime.of(year, month, day, hour, minute).toInstant(ZoneOffset.UTC));
    }

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        implSnapshotRepository = ctx.getBean(DbSnapshotRepository.class);
        implWhiteboardRepository = ctx.getBean(DbWhiteboardRepository.class);
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
    public void testCreate() throws SnapshotRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null);
        implSnapshotRepository.create(snapshot);
        Optional<SnapshotStatus> snapshotResolvedOptional =
            implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertTrue(snapshotResolvedOptional.isPresent());
        SnapshotStatus snapshotResolved = snapshotResolvedOptional.get();
        Assert.assertEquals(State.CREATED, snapshotResolved.state());
        Assert.assertEquals(snapshotOwner, snapshotResolved.snapshot().uid());
        Assert.assertEquals(creationDateUTC, snapshotResolved.snapshot().creationDateUTC());
        Assert.assertEquals(workflowName, snapshotResolved.snapshot().workflowName());
    }

    @Test
    public void testCreateFromSnapshot() throws SnapshotRepositoryException {
        Snapshot parentSnapshot =
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null);
        implSnapshotRepository.create(parentSnapshot);
        SnapshotEntry firstEntry = new SnapshotEntry.Impl(entryIdFirst, parentSnapshot);
        implSnapshotRepository.createEntry(parentSnapshot, entryIdFirst);
        implSnapshotRepository.prepare(firstEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(firstEntry, false);
        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, parentSnapshot);
        implSnapshotRepository.createEntry(parentSnapshot, entryIdSecond);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());

        String childSnapshotId = UUID.randomUUID().toString();
        Date childCreationDateUTC = Date.from(Instant.now());

        Snapshot childSnapshot =
            new Snapshot.Impl(URI.create(childSnapshotId), snapshotOwner, childCreationDateUTC, workflowName, null);
        implSnapshotRepository.createFromSnapshot(snapshotId, childSnapshot);

        Optional<SnapshotStatus> snapshotResolvedOptional =
            implSnapshotRepository.resolveSnapshot(URI.create(childSnapshotId));
        Assert.assertTrue(snapshotResolvedOptional.isPresent());
        SnapshotStatus snapshotResolved = snapshotResolvedOptional.get();
        Assert.assertEquals(State.CREATED, snapshotResolved.state());
        Assert.assertEquals(snapshotOwner, snapshotResolved.snapshot().uid());
        Assert.assertEquals(childCreationDateUTC, snapshotResolved.snapshot().creationDateUTC());
        Assert.assertEquals(workflowName, snapshotResolved.snapshot().workflowName());

        Optional<SnapshotEntryStatus> firstEntryStatusOptional =
            implSnapshotRepository.resolveEntryStatus(childSnapshot, entryIdFirst);
        Assert.assertTrue(firstEntryStatusOptional.isPresent());
        SnapshotEntryStatus firstEntryStatus = firstEntryStatusOptional.get();
        Assert.assertEquals(FINISHED, firstEntryStatus.status());
        Assert.assertEquals(storageUri, Objects.requireNonNull(firstEntryStatus.storage()).toString());
        Assert.assertFalse(firstEntryStatus.empty());

        Optional<SnapshotEntryStatus> secondEntryStatusOptional =
            implSnapshotRepository.resolveEntryStatus(childSnapshot, entryIdSecond);
        Assert.assertTrue(secondEntryStatusOptional.isEmpty());
    }

    @Test
    public void testResolveSnapshotNull() {
        Optional<SnapshotStatus> snapshotStatus =
            implSnapshotRepository.resolveSnapshot(URI.create(UUID.randomUUID().toString()));
        Assert.assertFalse(snapshotStatus.isPresent());
    }

    @Test
    public void testFinalizeSnapshotNotFound() {
        Assert.assertThrows(SnapshotRepositoryException.class,
            () -> implSnapshotRepository.finalize(
                new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null)));
    }

    @Test
    public void testFinalizeSnapshotErroredEntries() throws SnapshotRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null);
        implSnapshotRepository.create(snapshot);
        implSnapshotRepository.createEntry(snapshot, entryIdFirst);
        implSnapshotRepository.prepare(new SnapshotEntry.Impl(entryIdFirst, snapshot), storageUri,
            Collections.emptyList());
        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshot);
        implSnapshotRepository.createEntry(snapshot, entryIdSecond);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);
        implSnapshotRepository.finalize(snapshot);
        Optional<SnapshotStatus> statusOptional = implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertTrue(statusOptional.isPresent());
        SnapshotStatus status = statusOptional.get();
        Assert.assertEquals(SnapshotStatus.State.ERRORED, status.state());
    }

    @Test
    public void testFinalizeSnapshotOkEntries() throws SnapshotRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null);
        implSnapshotRepository.create(snapshot);
        SnapshotEntry firstEntry = new SnapshotEntry.Impl(entryIdFirst, snapshot);
        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshot);
        implSnapshotRepository.createEntry(snapshot, entryIdFirst);
        implSnapshotRepository.prepare(firstEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(firstEntry, false);
        implSnapshotRepository.createEntry(snapshot, entryIdSecond);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);
        implSnapshotRepository.finalize(snapshot);
        Optional<SnapshotStatus> statusOptional = implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertTrue(statusOptional.isPresent());
        SnapshotStatus status = statusOptional.get();
        Assert.assertEquals(FINALIZED, status.state());
    }

    @Test
    public void testFinalizeSnapshotNullStorageUriEntries() throws SnapshotRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null);
        implSnapshotRepository.create(snapshot);
        SnapshotEntry firstEntry = new SnapshotEntry.Impl(entryIdFirst, snapshot);
        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshot);
        implSnapshotRepository.createEntry(snapshot, entryIdFirst);
        implSnapshotRepository.prepare(firstEntry, null, Collections.emptyList());
        implSnapshotRepository.commit(firstEntry, false);
        implSnapshotRepository.createEntry(snapshot, entryIdSecond);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);
        implSnapshotRepository.finalize(snapshot);
        Optional<SnapshotStatus> statusOptional = implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertTrue(statusOptional.isPresent());
        SnapshotStatus status = statusOptional.get();
        Assert.assertEquals(SnapshotStatus.State.ERRORED, status.state());
    }

    @Test
    public void testErrorSnapshotNotFound() {
        Assert.assertThrows(SnapshotRepositoryException.class,
            () -> implSnapshotRepository.error(
                new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null)));
    }

    @Test
    public void testErrorSnapshot() throws SnapshotRepositoryException, WhiteboardRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null);
        implSnapshotRepository.create(snapshot);
        String namespace = "namespace";
        implWhiteboardRepository.create(
            new Impl(URI.create(wbIdFirst), Collections.emptySet(), snapshot, Collections.emptySet(), namespace,
                creationDateUTC)
        );
        implWhiteboardRepository.create(
            new Impl(URI.create(wbIdSecond), Collections.emptySet(), snapshot, Collections.emptySet(), namespace,
                creationDateUTC)
        );
        implSnapshotRepository.error(snapshot);
        WhiteboardStatus firstWhiteboard = implWhiteboardRepository.resolveWhiteboard(URI.create(wbIdFirst)).get();
        WhiteboardStatus secondWhiteboard = implWhiteboardRepository.resolveWhiteboard(URI.create(wbIdSecond)).get();
        Optional<SnapshotStatus> snapshotStatusOptional =
            implSnapshotRepository.resolveSnapshot(URI.create(snapshotId));
        Assert.assertTrue(snapshotStatusOptional.isPresent());
        SnapshotStatus snapshotStatus = snapshotStatusOptional.get();
        Assert.assertNotNull(snapshotStatus);
        Assert.assertEquals(State.ERRORED, snapshotStatus.state());
        Assert.assertNotNull(firstWhiteboard);
        Assert.assertEquals(ERRORED, firstWhiteboard.state());
        Assert.assertNotNull(secondWhiteboard);
        Assert.assertEquals(ERRORED, secondWhiteboard.state());
    }

    @Test
    public void testPrepareEntryExists() throws SnapshotRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null);
        implSnapshotRepository.create(snapshot);
        SnapshotEntry entry = new SnapshotEntry.Impl(entryIdFirst, snapshot);
        implSnapshotRepository.createEntry(snapshot, entryIdFirst);
        implSnapshotRepository.prepare(entry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(entry, false);
        Assert.assertThrows(RuntimeException.class,
            () -> implSnapshotRepository.prepare(entry, storageUri, Collections.emptyList()));
    }

    @Test
    public void testResolveEntryNotFound() {
        Assert.assertTrue(
            implSnapshotRepository.resolveEntry(
                new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null),
                entryIdFirst).isEmpty());
    }

    @Test
    public void testResolveEntryStatus() throws SnapshotRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null);
        implSnapshotRepository.create(snapshot);
        SnapshotEntry firstEntry = new SnapshotEntry.Impl(entryIdFirst, snapshot);
        implSnapshotRepository.createEntry(snapshot, entryIdFirst);
        implSnapshotRepository.prepare(firstEntry, storageUri, List.of(entryIdSecond, entryIdThird));
        implSnapshotRepository.commit(firstEntry, false);

        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshot);
        implSnapshotRepository.createEntry(snapshot, entryIdSecond);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);

        SnapshotEntry thirdEntry = new SnapshotEntry.Impl(entryIdThird, snapshot);
        implSnapshotRepository.createEntry(snapshot, entryIdThird);
        implSnapshotRepository.prepare(thirdEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(thirdEntry, false);

        Optional<SnapshotEntryStatus> snapshotEntryStatusOptional = implSnapshotRepository.resolveEntryStatus(
            new Snapshot.Impl(URI.create(snapshotId), snapshotOwner, creationDateUTC, workflowName, null), entryIdFirst
        );
        Assert.assertTrue(snapshotEntryStatusOptional.isPresent());
        SnapshotEntryStatus snapshotEntryStatus = snapshotEntryStatusOptional.get();
        Assert.assertEquals(snapshotId, snapshotEntryStatus.entry().snapshot().id().toString());
        Assert.assertEquals(entryIdFirst, snapshotEntryStatus.entry().id());
        Assert.assertNotNull(snapshotEntryStatus.storage());
        Assert.assertEquals(storageUri, Objects.requireNonNull(snapshotEntryStatus.storage()).toString());
        Assert.assertTrue(snapshotEntryStatus.dependentEntryIds().contains(entryIdSecond)
            && snapshotEntryStatus.dependentEntryIds().contains(entryIdThird));
    }

    @Test
    public void testCommitNotFound() {
        Assert.assertThrows(SnapshotRepositoryException.class,
            () -> implSnapshotRepository.commit(new SnapshotEntry.Impl(UUID.randomUUID().toString(),
                new Snapshot.Impl(URI.create(UUID.randomUUID().toString()), snapshotOwner, creationDateUTC,
                    workflowName, null)), true)
        );
    }

    @Test
    public void testLastSnapshotAny() throws SnapshotRepositoryException {
        String snapshotIdFinalized = UUID.randomUUID().toString();
        Snapshot snapshotFinalized =
            new Snapshot.Impl(URI.create(snapshotIdFinalized), snapshotOwner, createDateUTC(2000, 8, 5, 0, 0),
                workflowName, null);
        implSnapshotRepository.create(snapshotFinalized);
        implSnapshotRepository.finalize(snapshotFinalized);

        String snapshotIdCreated = UUID.randomUUID().toString();
        Snapshot snapshotCreated =
            new Snapshot.Impl(URI.create(snapshotIdCreated), snapshotOwner, createDateUTC(2002, 3, 2, 0, 0),
                workflowName, null);
        implSnapshotRepository.create(snapshotCreated);

        Optional<SnapshotStatus> snapshot = implSnapshotRepository.lastSnapshot(workflowName, snapshotOwner.toString());
        Assert.assertTrue(snapshot.isPresent());
        Assert.assertEquals(snapshot.get().snapshot().id().toString(), snapshotIdCreated);
    }

    @Test
    public void testLastSnapshotDifferentOwner() throws SnapshotRepositoryException {
        String snapshotIdCreated = UUID.randomUUID().toString();
        Snapshot snapshotCreated =
            new Snapshot.Impl(URI.create(snapshotIdCreated), snapshotOwner, createDateUTC(2002, 3, 2, 0, 0),
                workflowName, null);
        implSnapshotRepository.create(snapshotCreated);

        String snapshotIdFinalizedDifferentOwner = UUID.randomUUID().toString();
        Snapshot snapshotFinalizedDifferentOwner =
            new Snapshot.Impl(URI.create(snapshotIdFinalizedDifferentOwner), URI.create(UUID.randomUUID().toString()),
                createDateUTC(2005, 8, 5, 0, 0), workflowName, null);
        implSnapshotRepository.create(snapshotFinalizedDifferentOwner);
        implSnapshotRepository.finalize(snapshotFinalizedDifferentOwner);

        Optional<SnapshotStatus> snapshot =
            implSnapshotRepository.lastSnapshot(workflowName, snapshotOwner.toString());
        Assert.assertTrue(snapshot.isPresent());
        Assert.assertEquals(snapshot.get().snapshot().id().toString(), snapshotIdCreated);
    }
}
