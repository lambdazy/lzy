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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
    private final String storageUri = "storageUri";
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
                new Snapshot.Impl(URI.create(snapshotIdFirst))));

        WhiteboardModel whiteboardModel;
        WhiteboardFieldModel firstWhiteboardField;
        WhiteboardFieldModel secondWhiteboardField;
        try (Session session = storage.getSessionFactory().openSession()) {
            whiteboardModel = session.find(WhiteboardModel.class, wbIdFirst);
            firstWhiteboardField = session.find(WhiteboardFieldModel.class,
                    new WhiteboardFieldModel.WhiteboardFieldPk(wbIdFirst, fieldNameFirst));
            secondWhiteboardField = session.find(WhiteboardFieldModel.class,
                    new WhiteboardFieldModel.WhiteboardFieldPk(wbIdFirst, fieldNameSecond));
        }
        Assert.assertNotNull(whiteboardModel);
        Assert.assertEquals(snapshotIdFirst, whiteboardModel.getSnapshotId());
        Assert.assertEquals(WhiteboardStatus.State.CREATED, whiteboardModel.getWbState());
        Assert.assertNotNull(firstWhiteboardField);
        Assert.assertEquals(fieldNameFirst, firstWhiteboardField.getFieldName());
        Assert.assertNull(firstWhiteboardField.getEntryId());
        Assert.assertNotNull(secondWhiteboardField);
        Assert.assertEquals(fieldNameSecond, secondWhiteboardField.getFieldName());
        Assert.assertNull(secondWhiteboardField.getEntryId());
    }

    @Test
    public void testResolveWhiteboardNotFound() {
        Assert.assertNull(impl.resolveWhiteboard(URI.create(UUID.randomUUID().toString())));
    }

    @Test
    public void testResolveWhiteboard() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameFirst, null));
            session.save(new WhiteboardFieldModel(wbIdFirst, fieldNameSecond, entryIdSecond));
            session.save(new SnapshotModel(snapshotIdFirst, SnapshotStatus.State.CREATED));
            tx.commit();
        }
        WhiteboardStatus whiteboardStatus = impl.resolveWhiteboard(URI.create(wbIdFirst));
        Assert.assertNotNull(whiteboardStatus);
        Assert.assertEquals(CREATED, whiteboardStatus.state());
        Assert.assertEquals(snapshotIdFirst, whiteboardStatus.whiteboard().snapshot().id().toString());
        Assert.assertTrue(whiteboardStatus.whiteboard().fieldNames().contains(fieldNameFirst)
                && whiteboardStatus.whiteboard().fieldNames().contains(fieldNameSecond));
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
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst));
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
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst));
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
                        new Snapshot.Impl(URI.create(snapshotIdFirst))
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
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst));
            session.save(new WhiteboardModel(wbIdSecond, COMPLETED, snapshotIdFirst));
            session.save(new WhiteboardModel(wbIdThird, COMPLETED, snapshotIdSecond));
            session.save(new SnapshotOwnerModel(snapshotIdFirst, snapshotOwnerFirst));
            session.save(new SnapshotOwnerModel(snapshotIdSecond, snapshotOwnerSecond));
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
        Snapshot snapshot = new Snapshot.Impl(URI.create(snapshotId));
        return new WhiteboardField.Impl(fieldName, new SnapshotEntry.Impl(entryId, snapshot),
                new Whiteboard.Impl(URI.create(wbId), Collections.emptySet(), snapshot));
    }

    private void init() {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            session.save(new WhiteboardModel(wbIdFirst, CREATED, snapshotIdFirst));
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
            tx.commit();
        }
    }
}
