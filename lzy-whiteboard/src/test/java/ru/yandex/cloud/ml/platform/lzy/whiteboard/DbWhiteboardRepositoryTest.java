package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.micronaut.context.ApplicationContext;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard.Impl;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions.SnapshotRepositoryException;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions.WhiteboardRepositoryException;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbSnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbWhiteboardRepository;

public class DbWhiteboardRepositoryTest {

    private final String fieldNameFirst = "fieldNameFirst";
    private final String fieldNameSecond = "fieldNameSecond";
    private final String fieldNameThird = "fieldNameThird";
    private final String fieldNameFourth = "fieldNameFourth";
    private final String firstTag = "firstTag";
    private final String secondTag = "secondTag";
    private final String namespaceFirst = "namespaceFirst";
    private final String namespaceSecond = "namespaceSecond";
    private final String workflowName = "workflow";
    private final Date creationDateUTC = Date.from(Instant.now());
    private final Date creationDateUTCFrom = Date.from(LocalDateTime
        .of(1, 1, 1, 0, 0).toInstant(ZoneOffset.UTC));
    private final Date creationDateUTCTo = Date.from(LocalDateTime
        .of(9999, 1, 1, 0, 0).toInstant(ZoneOffset.UTC));
    DbWhiteboardRepository implWhiteboardRepository;
    DbSnapshotRepository implSnapshotRepository;
    private ApplicationContext ctx;
    private DbStorage storage;
    private String wbIdFirst;
    private String wbIdSecond;
    private String wbIdThird;
    private String snapshotIdFirst;
    private String snapshotIdSecond;
    private String entryIdFirst;
    private String entryIdSecond;
    private String entryIdThird;
    private String entryIdFourth;
    private String snapshotOwnerFirst;
    private String snapshotOwnerSecond;

    private Date createDateUTC(int year, int month, int day, int hour, int minute) {
        return Date.from(LocalDateTime.of(year, month, day, hour, minute).toInstant(ZoneOffset.UTC));
    }


    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        implWhiteboardRepository = ctx.getBean(DbWhiteboardRepository.class);
        implSnapshotRepository = ctx.getBean(DbSnapshotRepository.class);
        storage = ctx.getBean(DbStorage.class);

        wbIdFirst = UUID.randomUUID().toString();
        wbIdSecond = UUID.randomUUID().toString();
        wbIdThird = UUID.randomUUID().toString();
        snapshotIdFirst = UUID.randomUUID().toString();
        snapshotIdSecond = UUID.randomUUID().toString();
        entryIdFirst = UUID.randomUUID().toString();
        entryIdSecond = UUID.randomUUID().toString();
        entryIdThird = UUID.randomUUID().toString();
        entryIdFourth = UUID.randomUUID().toString();
        snapshotOwnerFirst = UUID.randomUUID().toString();
        snapshotOwnerSecond = UUID.randomUUID().toString();
    }

    @After
    public void tearDown() {
        ctx.stop();
    }

    @Test
    public void testCreate() throws SnapshotRepositoryException, WhiteboardRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotIdFirst), URI.create(snapshotOwnerFirst), creationDateUTC,
                workflowName, null);
        implSnapshotRepository.create(snapshot);
        implWhiteboardRepository.create(
            new Whiteboard.Impl(URI.create(wbIdFirst), Set.of(fieldNameFirst, fieldNameSecond),
                snapshot,
                Set.of(firstTag, secondTag), namespaceFirst, creationDateUTC));
        implSnapshotRepository.finalize(snapshot);
        WhiteboardStatus whiteboard = implWhiteboardRepository.resolveWhiteboard(URI.create(wbIdFirst)).get();
        Assert.assertNotNull(whiteboard);
        Assert.assertNotNull(whiteboard.whiteboard());
        Assert.assertNotNull(whiteboard.whiteboard().snapshot());
        Assert.assertEquals(snapshotIdFirst, whiteboard.whiteboard().snapshot().id().toString());
        Assert.assertEquals(namespaceFirst, whiteboard.whiteboard().namespace());

        Assert.assertTrue(whiteboard.whiteboard().fieldNames().contains(fieldNameFirst)
            && whiteboard.whiteboard().fieldNames().contains(fieldNameSecond));

        Assert.assertTrue(whiteboard.whiteboard().tags().contains(firstTag)
            && whiteboard.whiteboard().tags().contains(secondTag));
    }

    @Test
    public void testResolveWhiteboardByIdNotFound() {
        Assert.assertTrue(
            implWhiteboardRepository.resolveWhiteboard(URI.create(UUID.randomUUID().toString())).isEmpty());
    }

    @Test
    public void testResolveWhiteboardsNonMatchingTags()
        throws SnapshotRepositoryException, WhiteboardRepositoryException {
        init();
        String thirdTag = "thirdTag";
        List<WhiteboardStatus> whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceFirst, List.of(firstTag, secondTag, thirdTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(0, whiteboardStatusList.size());
        whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceFirst, List.of(firstTag, thirdTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(0, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsMatchingTags() throws SnapshotRepositoryException, WhiteboardRepositoryException {
        init();
        finalizeSnapshots();
        List<WhiteboardStatus> whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceFirst, List.of(firstTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsEmptyTags() throws SnapshotRepositoryException, WhiteboardRepositoryException {
        init();
        implWhiteboardRepository.create(
            new Whiteboard.Impl(
                URI.create(wbIdThird),
                Collections.emptySet(),
                new Snapshot.Impl(URI.create(snapshotIdFirst), URI.create(snapshotOwnerFirst), creationDateUTC,
                    workflowName, null),
                Collections.emptySet(),
                namespaceSecond,
                creationDateUTC
            ));
        finalizeSnapshots();
        List<WhiteboardStatus> whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceFirst, Collections.emptyList(), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsDifferentNamespace()
        throws SnapshotRepositoryException, WhiteboardRepositoryException {
        init();
        implWhiteboardRepository.create(
            new Whiteboard.Impl(
                URI.create(wbIdThird),
                Collections.emptySet(),
                new Snapshot.Impl(URI.create(snapshotIdFirst), URI.create(snapshotOwnerFirst), creationDateUTC,
                    workflowName, null),
                Set.of(firstTag, secondTag),
                namespaceSecond,
                creationDateUTC
            ));
        finalizeSnapshots();
        List<WhiteboardStatus> whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceFirst, List.of(firstTag, secondTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardStatusList.size());
        whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceSecond, List.of(firstTag, secondTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(1, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsFilterTime() throws SnapshotRepositoryException, WhiteboardRepositoryException {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotIdFirst), URI.create(snapshotOwnerFirst), creationDateUTC,
                workflowName, null);
        implSnapshotRepository.create(snapshot);
        implWhiteboardRepository.create(
            new Whiteboard.Impl(
                URI.create(wbIdFirst),
                Collections.emptySet(),
                snapshot,
                Set.of(firstTag, secondTag),
                namespaceFirst,
                createDateUTC(1982, 11, 14, 0, 0)
            ));
        implWhiteboardRepository.create(
            new Whiteboard.Impl(
                URI.create(wbIdSecond),
                Collections.emptySet(),
                snapshot,
                Set.of(firstTag, secondTag),
                namespaceFirst,
                createDateUTC(2021, 10, 13, 0, 0)
            ));
        implWhiteboardRepository.create(
            new Whiteboard.Impl(
                URI.create(wbIdThird),
                Collections.emptySet(),
                snapshot,
                Set.of(firstTag, secondTag),
                namespaceFirst,
                createDateUTC(1950, 8, 5, 0, 0)
            ));
        implSnapshotRepository.finalize(snapshot);
        List<WhiteboardStatus> whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceFirst, Collections.emptyList(), createDateUTC(1950, 8, 5, 0, 0), createDateUTC(2021, 10, 13, 0, 0)
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardStatusList.size());
        whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceFirst, Collections.emptyList(), createDateUTC(1950, 8, 5, 0, 0), createDateUTC(2021, 10, 14, 0, 0)
        ).collect(Collectors.toList());
        Assert.assertEquals(3, whiteboardStatusList.size());
        whiteboardStatusList = implWhiteboardRepository.resolveWhiteboards(
            namespaceFirst, Collections.emptyList(), createDateUTC(1951, 8, 5, 0, 0), createDateUTC(1963, 10, 14, 0, 0)
        ).collect(Collectors.toList());
        Assert.assertEquals(0, whiteboardStatusList.size());
    }

    @Test
    public void testAddFieldNotDeclared() {
        Assert.assertThrows(WhiteboardRepositoryException.class,
            () -> implWhiteboardRepository.update(
                createWhiteboardField(fieldNameFirst, entryIdFirst, snapshotIdFirst, wbIdFirst)));
    }

    @Test
    public void testDependent() throws SnapshotRepositoryException, WhiteboardRepositoryException {
        init();
        List<WhiteboardField> whiteboardFieldList = implWhiteboardRepository.dependent(
            createWhiteboardField(fieldNameFirst, entryIdFirst, snapshotIdFirst, wbIdFirst)
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardFieldList.size());
        WhiteboardField whiteboardFieldFirst = whiteboardFieldList.get(0);
        WhiteboardField whiteboardFieldSecond = whiteboardFieldList.get(1);
        Assert.assertTrue(whiteboardFieldFirst.name().equals(fieldNameSecond)
            || whiteboardFieldFirst.name().equals(fieldNameThird));

        if (whiteboardFieldFirst.name().equals(fieldNameSecond)) {
            Assert.assertEquals(entryIdSecond, Objects.requireNonNull(whiteboardFieldFirst.entry()).id());
            Assert.assertEquals(entryIdThird, Objects.requireNonNull(whiteboardFieldSecond.entry()).id());
            Assert.assertEquals(fieldNameThird, whiteboardFieldSecond.name());
        } else {
            Assert.assertEquals(entryIdThird, Objects.requireNonNull(whiteboardFieldFirst.entry()).id());
            Assert.assertEquals(entryIdSecond, Objects.requireNonNull(whiteboardFieldSecond.entry()).id());
            Assert.assertEquals(fieldNameSecond, whiteboardFieldSecond.name());
        }

        whiteboardFieldList = implWhiteboardRepository.dependent(
            createWhiteboardField(fieldNameFourth, entryIdFourth, snapshotIdFirst, wbIdFirst)
        ).collect(Collectors.toList());
        Assert.assertEquals(1, whiteboardFieldList.size());
        Assert.assertTrue(whiteboardFieldList.get(0).name().equals(fieldNameFirst)
            && Objects.requireNonNull(whiteboardFieldList.get(0).entry()).id().equals(entryIdFirst)
        );
    }

    @Test
    public void testFields() throws SnapshotRepositoryException, WhiteboardRepositoryException {
        init();
        List<WhiteboardField> whiteboardFieldList = implWhiteboardRepository.fields(
            new Whiteboard.Impl(
                URI.create(wbIdFirst),
                Set.of(fieldNameFirst, fieldNameSecond, fieldNameThird, fieldNameFourth),
                new Snapshot.Impl(URI.create(snapshotIdFirst), URI.create(snapshotOwnerFirst), creationDateUTC,
                    workflowName, null),
                Set.of(firstTag, secondTag),
                namespaceFirst,
                creationDateUTC
            )
        ).collect(Collectors.toList());
        Assert.assertEquals(4, whiteboardFieldList.size());
        boolean firstFieldPresent = false;
        boolean secondFieldPresent = false;
        boolean thirdFieldPresent = false;
        boolean fourthFieldPresent = false;
        for (WhiteboardField wbField : whiteboardFieldList) {
            switch (wbField.name()) {
                case (fieldNameFirst):
                    firstFieldPresent = true;
                    Assert.assertEquals(entryIdFirst, Objects.requireNonNull(wbField.entry()).id());
                    break;
                case (fieldNameSecond):
                    secondFieldPresent = true;
                    Assert.assertEquals(entryIdSecond, Objects.requireNonNull(wbField.entry()).id());
                    break;
                case (fieldNameThird):
                    thirdFieldPresent = true;
                    Assert.assertEquals(entryIdThird, Objects.requireNonNull(wbField.entry()).id());
                    break;
                case (fieldNameFourth):
                    fourthFieldPresent = true;
                    Assert.assertEquals(entryIdFourth, Objects.requireNonNull(wbField.entry()).id());
                    break;
                default:
                    Assert.fail();
            }
        }
        Assert.assertTrue(firstFieldPresent && secondFieldPresent && thirdFieldPresent && fourthFieldPresent);
    }

    private WhiteboardField createWhiteboardField(
        String fieldName, String entryId, String snapshotId, String wbId
    ) {
        Snapshot snapshot =
            new Snapshot.Impl(URI.create(snapshotId), URI.create(snapshotOwnerFirst), creationDateUTC, workflowName,
                null);
        return new WhiteboardField.Impl(fieldName, new SnapshotEntry.Impl(entryId, snapshot),
            new Whiteboard.Impl(URI.create(wbId), Collections.emptySet(), snapshot,
                Collections.emptySet(), namespaceFirst, creationDateUTC));
    }

    private void init() throws SnapshotRepositoryException, WhiteboardRepositoryException {
        Snapshot snapshotFirst = new Snapshot.Impl(
            URI.create(snapshotIdFirst),
            URI.create(snapshotOwnerFirst),
            creationDateUTC,
            workflowName,
            null
        );
        implSnapshotRepository.create(snapshotFirst);
        Whiteboard whiteboardFirst = new Impl(
            URI.create(wbIdFirst),
            Set.of(fieldNameFirst, fieldNameSecond, fieldNameThird, fieldNameFourth),
            snapshotFirst,
            Set.of(firstTag, secondTag),
            namespaceFirst,
            creationDateUTC
        );
        implWhiteboardRepository.create(whiteboardFirst);
        SnapshotEntry firstEntry = new SnapshotEntry.Impl(entryIdFirst, snapshotFirst);
        implSnapshotRepository.createEntry(snapshotFirst, entryIdFirst);
        SnapshotEntry fourthEntry = new SnapshotEntry.Impl(entryIdFourth, snapshotFirst);
        implSnapshotRepository.createEntry(snapshotFirst, entryIdFourth);
        SnapshotEntry secondEntry = new SnapshotEntry.Impl(entryIdSecond, snapshotFirst);
        implSnapshotRepository.createEntry(snapshotFirst, entryIdSecond);
        SnapshotEntry thirdEntry = new SnapshotEntry.Impl(entryIdThird, snapshotFirst);
        implSnapshotRepository.createEntry(snapshotFirst, entryIdThird);
        String storageUri = "storageUri";
        implSnapshotRepository.prepare(firstEntry, storageUri, List.of(entryIdSecond, entryIdThird));
        implSnapshotRepository.commit(firstEntry, false);
        implSnapshotRepository.prepare(fourthEntry, storageUri, List.of(entryIdFirst));
        implSnapshotRepository.commit(fourthEntry, false);
        implSnapshotRepository.prepare(secondEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(secondEntry, false);
        implSnapshotRepository.prepare(thirdEntry, storageUri, Collections.emptyList());
        implSnapshotRepository.commit(thirdEntry, false);
        implWhiteboardRepository.update(new WhiteboardField.Impl(fieldNameFirst, firstEntry, whiteboardFirst));
        implWhiteboardRepository.update(new WhiteboardField.Impl(fieldNameSecond, secondEntry, whiteboardFirst));
        implWhiteboardRepository.update(new WhiteboardField.Impl(fieldNameThird, thirdEntry, whiteboardFirst));
        implWhiteboardRepository.update(new WhiteboardField.Impl(fieldNameFourth, fourthEntry, whiteboardFirst));

        Snapshot snapshotSecond = new Snapshot.Impl(
            URI.create(snapshotIdSecond),
            URI.create(snapshotOwnerFirst),
            creationDateUTC,
            workflowName,
            null
        );
        implSnapshotRepository.create(snapshotSecond);
        Whiteboard whiteboardSecond = new Impl(
            URI.create(wbIdSecond),
            Set.of(fieldNameSecond),
            snapshotSecond,
            Set.of(firstTag, secondTag),
            namespaceFirst,
            creationDateUTC
        );
        implWhiteboardRepository.create(whiteboardSecond);
        implWhiteboardRepository.update(new WhiteboardField.Impl(fieldNameSecond, secondEntry, whiteboardSecond));
    }

    private void finalizeSnapshots() throws SnapshotRepositoryException {
        implSnapshotRepository.finalize(new Snapshot.Impl(
            URI.create(snapshotIdFirst),
            URI.create(snapshotOwnerFirst),
            creationDateUTC,
            workflowName,
            null
        ));
        implSnapshotRepository.finalize(new Snapshot.Impl(
            URI.create(snapshotIdSecond),
            URI.create(snapshotOwnerFirst),
            creationDateUTC,
            workflowName,
            null
        ));
    }
}
