package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.micronaut.context.ApplicationContext;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.mem.WhiteboardRepositoryImpl;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus.State.IN_PROGRESS;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.COMPLETED;
import static ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus.State.CREATED;

public class WhiteboardRepositoryImplTest {
    private ApplicationContext ctx;
    WhiteboardRepositoryImpl impl;
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

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        impl = ctx.getBean(WhiteboardRepositoryImpl.class);
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
                Set.of(firstTag, secondTag), namespaceFirst));

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
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst));
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
    public void testResolveWhiteboardsByNamespaceAndTagsMultipleWhiteboards() {
        init();
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(namespaceFirst, List.of(firstTag, secondTag));
        Assert.assertEquals(2, whiteboardStatusList.size());
        Assert.assertTrue(Objects.equals(whiteboardStatusList.get(0).whiteboard().id().toString(), wbIdFirst) &&
                Objects.equals(whiteboardStatusList.get(1).whiteboard().id().toString(), wbIdSecond) ||
                Objects.equals(whiteboardStatusList.get(1).whiteboard().id().toString(), wbIdFirst) &&
                        Objects.equals(whiteboardStatusList.get(0).whiteboard().id().toString(), wbIdSecond));
    }

    @Test
    public void testResolveWhiteboardsByNamespaceAndTagsNonMatchingTags() {
        init();
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
                namespaceFirst, List.of(firstTag, secondTag, thirdTag)
        );
        Assert.assertEquals(0, whiteboardStatusList.size());
        whiteboardStatusList = impl.resolveWhiteboards(
                namespaceFirst, List.of(firstTag, thirdTag)
        );
        Assert.assertEquals(0, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsByNamespaceAndTagsMatchingTags() {
        init();
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
                namespaceFirst, List.of(firstTag)
        );
        Assert.assertEquals(2, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsByNamespaceAndTagsEmptyTags() {
        init();
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdThird, CREATED, snapshotIdFirst, namespaceFirst));
            tx.commit();
        }
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
                namespaceFirst, Collections.emptyList()
        );
        Assert.assertEquals(3, whiteboardStatusList.size());
    }

    @Test
    public void testResolveWhiteboardsByNamespaceAndTagsDifferentNamespace() {
        init();
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdThird, CREATED, snapshotIdFirst, namespaceSecond));
            session.save(new WhiteboardTagModel(wbIdThird, firstTag));
            session.save(new WhiteboardTagModel(wbIdThird, secondTag));
            tx.commit();
        }
        List<WhiteboardStatus> whiteboardStatusList = impl.resolveWhiteboards(
                namespaceFirst, List.of(firstTag, secondTag)
        );
        Assert.assertEquals(2, whiteboardStatusList.size());
        whiteboardStatusList = impl.resolveWhiteboards(
                namespaceSecond, List.of(firstTag, secondTag)
        );
        Assert.assertEquals(1, whiteboardStatusList.size());
    }

    @Test
    public void testAddFieldNotDeclared() {
        Assert.assertThrows(RuntimeException.class,
                () -> impl.add(createWhiteboardField(fieldNameFirst, entryIdFirst, snapshotIdFirst, wbIdFirst)));
    }

    @Test
    public void testAddField() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFirst, null));
            tx.commit();
        }
        impl.add(createWhiteboardField(fieldNameFirst, entryIdFirst, snapshotIdFirst, wbIdFirst));
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
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst));
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
                        namespaceFirst
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

    @Test
    public void testWhiteboards() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst));
            session.save(new WhiteboardModel(wbIdSecond, COMPLETED, snapshotIdFirst, namespaceFirst));
            session.save(new WhiteboardModel(wbIdThird, COMPLETED, snapshotIdSecond, namespaceFirst));
            session.save(new SnapshotModel(snapshotIdFirst, SnapshotStatus.State.FINALIZED, snapshotOwnerFirst));
            session.save(new SnapshotModel(snapshotIdSecond, SnapshotStatus.State.FINALIZED, snapshotOwnerSecond));
            tx.commit();
        }
        List<WhiteboardInfo> resultFirstUser = impl.whiteboards(URI.create(snapshotOwnerFirst));
        Assert.assertEquals(2, resultFirstUser.size());
        Assert.assertTrue(
            resultFirstUser.get(0).id().equals(URI.create(wbIdFirst)) && resultFirstUser.get(0).state().equals(CREATED) &&
                    resultFirstUser.get(1).id().equals(URI.create(wbIdSecond)) && resultFirstUser.get(1).state().equals(COMPLETED) ||
                    resultFirstUser.get(1).id().equals(URI.create(wbIdFirst)) && resultFirstUser.get(1).state().equals(CREATED) &&
                    resultFirstUser.get(0).id().equals(URI.create(wbIdSecond)) && resultFirstUser.get(0).state().equals(COMPLETED)
        );
        List<WhiteboardInfo> resultSecondUser = impl.whiteboards(URI.create(snapshotOwnerSecond));
        Assert.assertEquals(1, resultSecondUser.size());
        Assert.assertTrue(resultSecondUser.get(0).id().equals(URI.create(wbIdThird)) && resultSecondUser.get(0).state().equals(COMPLETED));
    }

    @Test
    public void testWhiteboardsEmpty() {
        List<WhiteboardInfo> result = impl.whiteboards(URI.create(UUID.randomUUID().toString()));
        Assert.assertEquals(0, result.size());
    }

    private WhiteboardField createWhiteboardField(
            String fieldName, String entryId, String snapshotId, String wbId
    ) {
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId), URI.create(snapshotOwnerFirst));
        return new WhiteboardField.Impl(fieldName, new SnapshotEntry.Impl(entryId, snapshot),
                new Whiteboard.Impl(URI.create(wbId), Collections.emptySet(), snapshot, Collections.emptySet(), namespaceFirst));
    }

    private void init() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst, namespaceFirst));
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
            session.save(new WhiteboardModel(wbIdSecond, COMPLETED, snapshotIdSecond, namespaceFirst));
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
