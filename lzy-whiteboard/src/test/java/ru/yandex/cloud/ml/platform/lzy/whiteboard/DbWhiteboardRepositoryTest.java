package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.micronaut.context.ApplicationContext;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbWhiteboardRepository;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus.State.IN_PROGRESS;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.COMPLETED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.CREATED;

public class DbWhiteboardRepositoryTest {
    private ApplicationContext ctx;
    DbWhiteboardRepository impl;
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
    private final String fieldNameFirst = "fieldNameFirst";
    private final String fieldNameSecond = "fieldNameSecond";
    private final String fieldNameThird = "fieldNameThird";
    private final String fieldNameFourth = "fieldNameFourth";
    private final String firstTag = "firstTag";
    private final String secondTag = "secondTag";
    private final String thirdTag = "thirdTag";
    private final String storageUri = "storageUri";
    private final String namespaceFirst = "namespaceFirst";
    private final String namespaceSecond = "namespaceSecond";
    private String snapshotOwnerFirst;
    private String snapshotOwnerSecond;
    private final Date creationDateUTC = Date.from(Instant.now());
    private final Date creationDateUTCFrom = Date.from(LocalDateTime
        .of(1, 1, 1, 0, 0).toInstant(ZoneOffset.UTC));
    private final Date creationDateUTCTo = Date.from(LocalDateTime
        .of(9999, 1, 1, 0, 0).toInstant(ZoneOffset.UTC));

    private Date createDateUTC(int year, int month, int day, int hour, int minute) {
        return Date.from(LocalDateTime.of(year, month, day, hour, minute).toInstant(ZoneOffset.UTC));
    }

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        impl = ctx.getBean(DbWhiteboardRepository.class);
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
    public void testCreate() {
        impl.create(new Whiteboard.Impl(URI.create(wbIdFirst), Set.of(fieldNameFirst, fieldNameSecond),
                new Snapshot.Impl(URI.create(snapshotIdFirst), URI.create(snapshotOwnerFirst)),
                Set.of(firstTag, secondTag), namespaceFirst, creationDateUTC));

        WhiteboardModel whiteboardModel;
        WhiteboardFieldModel firstWhiteboardField;
        WhiteboardFieldModel secondWhiteboardField;
        WhiteboardTagModel firstWhiteboardTag;
        WhiteboardTagModel secondWhiteboardTag;
        try (Session session = storage.getSessionFactory().openSession()) {
            whiteboardModel = session.find(WhiteboardModel.class, wbIdFirst);
            firstWhiteboardField = session.find(WhiteboardFieldModel.class,
                    new WhiteboardFieldModel.WhiteboardFieldPk(wbIdFirst, fieldNameFirst));
            secondWhiteboardField = session.find(WhiteboardFieldModel.class,
                    new WhiteboardFieldModel.WhiteboardFieldPk(wbIdFirst, fieldNameSecond));
            firstWhiteboardTag = session.find(WhiteboardTagModel.class,
                    new WhiteboardTagModel.WhiteboardTagPk(wbIdFirst, firstTag));
            secondWhiteboardTag = session.find(WhiteboardTagModel.class,
                    new WhiteboardTagModel.WhiteboardTagPk(wbIdFirst, secondTag));
        }
        Assert.assertNotNull(whiteboardModel);
        Assert.assertEquals(snapshotIdFirst, whiteboardModel.getSnapshotId());
        Assert.assertEquals(WhiteboardStatus.State.CREATED, whiteboardModel.getWbState());
        Assert.assertEquals(namespaceFirst, whiteboardModel.getNamespace());

        Assert.assertNotNull(firstWhiteboardField);
        Assert.assertEquals(fieldNameFirst, firstWhiteboardField.getFieldName());
        Assert.assertNull(firstWhiteboardField.getEntryId());
        Assert.assertNotNull(secondWhiteboardField);
        Assert.assertEquals(fieldNameSecond, secondWhiteboardField.getFieldName());
        Assert.assertNull(secondWhiteboardField.getEntryId());

        Assert.assertNotNull(firstWhiteboardTag);
        Assert.assertEquals(firstTag, firstWhiteboardTag.getTag());
        Assert.assertNotNull(secondWhiteboardTag);
        Assert.assertEquals(secondTag, secondWhiteboardTag.getTag());
    }

    @Test
    public void testResolveWhiteboardByIdNotFound() {
        Assert.assertNull(impl.resolveWhiteboard(URI.create(UUID.randomUUID().toString())));
    }

    @Test
    public void testResolveWhiteboardById() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst, creationDateUTC));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFirst, null));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameSecond, entryIdSecond));
            session.save(new WhiteboardTagModel(wbIdFirst,firstTag));
            session.save(new WhiteboardTagModel(wbIdFirst,secondTag));
            session.save(new SnapshotModel(snapshotIdFirst, SnapshotStatus.State.CREATED, snapshotOwnerFirst));
            tx.commit();
        }
        WhiteboardStatus whiteboardStatus = impl.resolveWhiteboard(URI.create(wbIdFirst));
        Assert.assertNotNull(whiteboardStatus);
        Assert.assertEquals(CREATED, whiteboardStatus.state());
        Assert.assertEquals(snapshotIdFirst, whiteboardStatus.whiteboard().snapshot().id().toString());
        Assert.assertEquals(namespaceFirst, whiteboardStatus.whiteboard().namespace());
        Assert.assertTrue(whiteboardStatus.whiteboard().fieldNames().contains(fieldNameFirst)
                && whiteboardStatus.whiteboard().fieldNames().contains(fieldNameSecond));
        Assert.assertTrue(whiteboardStatus.whiteboard().tags().contains(firstTag)
                && whiteboardStatus.whiteboard().tags().contains(secondTag));
    }

    @Test
    public void testResolveWhiteboardsMultipleWhiteboards() {
        init();
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, List.of(firstTag, secondTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardStatusList.size());
        Assert.assertTrue(Objects.equals(whiteboardStatusList.get(0).whiteboard().id().toString(), wbIdFirst) &&
                Objects.equals(whiteboardStatusList.get(1).whiteboard().id().toString(), wbIdSecond) ||
                Objects.equals(whiteboardStatusList.get(1).whiteboard().id().toString(), wbIdFirst) &&
                        Objects.equals(whiteboardStatusList.get(0).whiteboard().id().toString(), wbIdSecond));
    }

    @Test
    public void testResolveWhiteboardsNonMatchingTags() {
        init();
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, List.of(firstTag, secondTag, thirdTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(0, whiteboardStatusList.size());
        whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, List.of(firstTag, thirdTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(0, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsMatchingTags() {
        init();
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, List.of(firstTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsEmptyTags() {
        init();
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdThird, CREATED, snapshotIdFirst, namespaceFirst, creationDateUTC));
            tx.commit();
        }
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, Collections.emptyList(), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(3, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsDifferentNamespace() {
        init();
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdThird, CREATED, snapshotIdFirst, namespaceSecond, creationDateUTC));
            session.save(new WhiteboardTagModel(wbIdThird, firstTag));
            session.save(new WhiteboardTagModel(wbIdThird, secondTag));
            tx.commit();
        }
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, List.of(firstTag, secondTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardStatusList.size());
        whiteboardStatusList = impl.resolveWhiteboards(
            namespaceSecond, List.of(firstTag, secondTag), creationDateUTCFrom, creationDateUTCTo
        ).collect(Collectors.toList());
        Assert.assertEquals(1, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsFilterTime() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst, createDateUTC(1982, 11, 14, 0, 0)));
            session.save(new WhiteboardModel(wbIdSecond, CREATED, snapshotIdFirst, namespaceFirst, createDateUTC(2021, 10, 13, 0, 0)));
            session.save(new WhiteboardModel(wbIdThird, CREATED, snapshotIdFirst, namespaceFirst, createDateUTC(1950, 8, 5, 0, 0)));
            tx.commit();
        }
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, Collections.emptyList(), createDateUTC(1950, 8, 5, 0, 0), createDateUTC(2021, 10, 13, 0, 0)
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardStatusList.size());
        whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, Collections.emptyList(), createDateUTC(1950, 8, 5, 0, 0), createDateUTC(2021, 10, 14, 0, 0)
        ).collect(Collectors.toList());
        Assert.assertEquals(3, whiteboardStatusList.size());
        whiteboardStatusList = impl.resolveWhiteboards(
            namespaceFirst, Collections.emptyList(), createDateUTC(1951, 8, 5, 0, 0), createDateUTC(1963, 10, 14, 0, 0)
        ).collect(Collectors.toList());
        Assert.assertEquals(0, whiteboardStatusList.size());
    }

    @Test
    public void testAddFieldNotDeclared() {
        Assert.assertThrows(RuntimeException.class,
                () -> impl.update(createWhiteboardField(fieldNameFirst, entryIdFirst, snapshotIdFirst, wbIdFirst)));
    }

    @Test
    public void testAddField() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst, creationDateUTC));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFirst, null));
            tx.commit();
        }
        impl.update(createWhiteboardField(fieldNameFirst, entryIdFirst, snapshotIdFirst, wbIdFirst));
        WhiteboardFieldModel whiteboardFieldModel;
        try (Session session = storage.getSessionFactory().openSession()) {
            whiteboardFieldModel = session.find(WhiteboardFieldModel.class,
                    new WhiteboardFieldModel.WhiteboardFieldPk(wbIdFirst, fieldNameFirst));
        }
        Assert.assertEquals(entryIdFirst, whiteboardFieldModel.getEntryId());
    }

    @Test
    public void testDependentEntryIdNotBound() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst, creationDateUTC));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFirst, null));
            tx.commit();
        }
        Stream<WhiteboardField> whiteboardFieldStream = impl.dependent(
                createWhiteboardField(fieldNameFirst, null, snapshotIdFirst, wbIdFirst)
        );
        Assert.assertEquals(0, whiteboardFieldStream.count());
    }

    @Test
    public void testDependent() {
        init();
        List<WhiteboardField> whiteboardFieldList = impl.dependent(
                createWhiteboardField(fieldNameFirst, entryIdFirst, snapshotIdFirst, wbIdFirst)
        ).collect(Collectors.toList());
        Assert.assertEquals(2, whiteboardFieldList.size());
        WhiteboardField whiteboardFieldFirst = whiteboardFieldList.get(0);
        WhiteboardField whiteboardFieldSecond = whiteboardFieldList.get(1);
        Assert.assertTrue(whiteboardFieldFirst.name().equals(fieldNameSecond)
                || whiteboardFieldFirst.name().equals(fieldNameThird));

        if (whiteboardFieldFirst.name().equals(fieldNameSecond)) {
            Assert.assertEquals(entryIdSecond, whiteboardFieldFirst.entry().id());
            Assert.assertEquals(entryIdThird, whiteboardFieldSecond.entry().id());
            Assert.assertEquals(fieldNameThird, whiteboardFieldSecond.name());
        } else {
            Assert.assertEquals(entryIdThird, whiteboardFieldFirst.entry().id());
            Assert.assertEquals(entryIdSecond, whiteboardFieldSecond.entry().id());
            Assert.assertEquals(fieldNameSecond, whiteboardFieldSecond.name());
        }

        whiteboardFieldList = impl.dependent(
                createWhiteboardField(fieldNameFourth, entryIdFourth, snapshotIdFirst, wbIdFirst)
        ).collect(Collectors.toList());
        Assert.assertEquals(1, whiteboardFieldList.size());
        Assert.assertTrue(whiteboardFieldList.get(0).name().equals(fieldNameFirst)
                && whiteboardFieldList.get(0).entry().id().equals(entryIdFirst)
        );
    }

    @Test
    public void testFields() {
        init();
        List<WhiteboardField> whiteboardFieldList = impl.fields(
                new Whiteboard.Impl(
                    URI.create(wbIdFirst),
                    Set.of(fieldNameFirst, fieldNameSecond, fieldNameThird, fieldNameFourth),
                    new Snapshot.Impl(URI.create(snapshotIdFirst), URI.create(snapshotOwnerFirst)),
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
                    Assert.assertEquals(entryIdFirst, wbField.entry().id());
                    break;
                case (fieldNameSecond):
                    secondFieldPresent = true;
                    Assert.assertEquals(entryIdSecond, wbField.entry().id());
                    break;
                case (fieldNameThird):
                    thirdFieldPresent = true;
                    Assert.assertEquals(entryIdThird, wbField.entry().id());
                    break;
                case (fieldNameFourth):
                    fourthFieldPresent = true;
                    Assert.assertEquals(entryIdFourth, wbField.entry().id());
                    break;
                default:
                    Assert.fail();
                    break;
            }
        }
        Assert.assertTrue(firstFieldPresent && secondFieldPresent && thirdFieldPresent && fourthFieldPresent);
    }

    private WhiteboardField createWhiteboardField(
            String fieldName, String entryId, String snapshotId, String wbId
    ) {
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), URI.create(snapshotOwnerFirst));
        return new WhiteboardField.Impl(fieldName, new SnapshotEntry.Impl(entryId, snapshot),
                new Whiteboard.Impl(URI.create(wbId), Collections.emptySet(), snapshot,
                    Collections.emptySet(), namespaceFirst, creationDateUTC));
    }

    private void init() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst, creationDateUTC));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFirst, entryIdFirst));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameSecond, entryIdSecond));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameThird, entryIdThird));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFourth, entryIdFourth));
            session.save(new EntryDependenciesModel(snapshotIdFirst, entryIdSecond, entryIdFirst));
            session.save(new EntryDependenciesModel(snapshotIdFirst, entryIdThird, entryIdFirst));
            session.save(new EntryDependenciesModel(snapshotIdFirst, entryIdFirst, entryIdFourth));
            session.save(new SnapshotEntryModel(snapshotIdFirst, entryIdFirst, storageUri, true, IN_PROGRESS));
            session.save(new SnapshotEntryModel(snapshotIdFirst, entryIdSecond, storageUri, true, IN_PROGRESS));
            session.save(new SnapshotEntryModel(snapshotIdFirst, entryIdThird, storageUri, true, IN_PROGRESS));
            session.save(new SnapshotEntryModel(snapshotIdFirst, entryIdFourth, storageUri, true, IN_PROGRESS));
            session.save(new WhiteboardModel(wbIdSecond, COMPLETED, snapshotIdSecond, namespaceFirst, creationDateUTC));
            session.save(new WhiteboardFieldModel(wbIdSecond, fieldNameSecond, entryIdSecond));
            session.save(new WhiteboardTagModel(wbIdFirst, firstTag));
            session.save(new WhiteboardTagModel(wbIdFirst, secondTag));
            session.save(new WhiteboardTagModel(wbIdSecond, firstTag));
            session.save(new WhiteboardTagModel(wbIdSecond, secondTag));
            session.save(new SnapshotModel(snapshotIdFirst, SnapshotStatus.State.CREATED, snapshotOwnerFirst));
            session.save(new SnapshotModel(snapshotIdSecond, SnapshotStatus.State.CREATED, snapshotOwnerFirst));
            tx.commit();
        }
    }
}
