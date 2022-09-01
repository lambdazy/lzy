package ai.lzy.whiteboard;

import ai.lzy.model.data.DataSchema;
import ai.lzy.model.data.types.SchemeType;
import ai.lzy.model.db.NotFoundException;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import ai.lzy.whiteboard.storage.WhiteboardStorage;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class StorageTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private WhiteboardStorage wbStorage;
    private Storage dataSource;
    private ApplicationContext context;

    @Before
    public void setUp() {
        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("whiteboard", db.getConnectionInfo()));
        wbStorage = context.getBean(WhiteboardStorage.class);
        dataSource = context.getBean(WhiteboardDataSource.class);
    }

    @After
    public void tearDown() {
        DatabaseTestUtils.cleanup(context.getBean(WhiteboardDataSource.class));
        context.stop();
    }

    @Test
    public void insertAndGetWhiteboard() throws SQLException {
        final var userId = "uid-42";

        final var wb = new Whiteboard(
            "id1", "wb-name-1", Set.of("f1", "f2", "f3"), Set.of(), Set.of(),
            new Whiteboard.Storage("s-name", ""), "namespace",
            Whiteboard.Status.CREATED, Instant.now()
        );

        wbStorage.insertWhiteboard(userId, wb, null);
        Assert.assertEquals(wb, wbStorage.getWhiteboard(userId, wb.id(), null));

        final var wbTagged = new Whiteboard(
            "id2", "wb-name-1", Set.of("f1"), Set.of(), Set.of("lol", "kek", "cheburek"),
            new Whiteboard.Storage("s-name", "My super secret storage."), "namespace",
            Whiteboard.Status.CREATED, Instant.now()
        );

        wbStorage.insertWhiteboard(userId, wbTagged, null);
        Assert.assertEquals(wbTagged, wbStorage.getWhiteboard(userId, wbTagged.id(), null));
    }

    @Test
    public void linkWhiteboardFields() throws SQLException {
        final var userId = "uid-42";

        Whiteboard wb = genWhiteboard("id", "wb-name", Set.of("f1", "f2"), Set.of("t1"), Instant.now());
        wbStorage.insertWhiteboard(userId, wb, null);

        final var f1 = genLinkedField("f1");
        final var f2 = genLinkedField("f2");
        final var f0 = genLinkedField("f0");

        wbStorage.markFieldLinked(wb.id(), f1, Instant.now(), null);
        wb = wbStorage.getWhiteboard(userId, wb.id(), null);
        Assert.assertEquals(1, wb.createdFieldNames().size());
        Assert.assertEquals(1, wb.linkedFields().size());
        Assert.assertTrue(wb.linkedFields().contains(f1));
        Assert.assertTrue(wb.hasField("f2") && !wb.hasLinkedField("f2"));

        try {
            wbStorage.markFieldLinked(wb.id(), f0, Instant.now(), null);
            Assert.fail("Field doesn't exist, but has successfully linked");
        } catch (NotFoundException e) {
            // ignored
        } catch (Exception e) {
            Assert.fail("Unexpected exception");
        }

        wbStorage.markFieldLinked(wb.id(), f2, Instant.now(), null);
        wb = wbStorage.getWhiteboard(userId, wb.id(), null);
        Assert.assertEquals(0, wb.createdFieldNames().size());
        Assert.assertEquals(2, wb.linkedFields().size());
        Assert.assertTrue(wb.linkedFields().contains(f1));
        Assert.assertTrue(wb.linkedFields().contains(f2));
    }


    @Test
    public void finalizeWhiteboard() throws SQLException {
        final var userId = "uid-42";

        final var wb = genWhiteboard("id", "wb-name", Set.of("f1", "f2"), Set.of("t1"), Instant.now());
        wbStorage.insertWhiteboard(userId, wb, null);

        final var f1 = genLinkedField("f1");
        final var f2 = genLinkedField("f2");
        wbStorage.markFieldLinked(wb.id(), f1, Instant.now(), null);
        wbStorage.markFieldLinked(wb.id(), f2, Instant.now(), null);

        wbStorage.setWhiteboardFinalized(wb.id(), Instant.now(), null);

        final var expectedFinalizedWb = new Whiteboard(
            wb.id(), wb.name(), Set.of(), Set.of(f1, f2), wb.tags(),
            wb.storage(), wb.namespace(), Whiteboard.Status.FINALIZED, wb.createdAt());
        final var finalizedWb = wbStorage.getWhiteboard(userId, wb.id(), null);
        Assert.assertEquals(expectedFinalizedWb, finalizedWb);
    }

    @Test
    public void listWhiteboards() throws SQLException {
        final var userId1 = "uid1";
        final var userId2 = "uid2";

        final var wb1 = genWhiteboard("id1", "name1", Set.of("f"), Set.of("all","b","c","x"),
            Instant.parse("2022-09-01T12:00:00.00Z"));
        wbStorage.insertWhiteboard(userId1, wb1, null);
        wbStorage.markFieldLinked(wb1.id(), genLinkedField("f"), wb1.createdAt().plusSeconds(30), null);
        wbStorage.setWhiteboardFinalized(wb1.id(), wb1.createdAt().plusSeconds(60), null);

        final var wb2 = genWhiteboard("id2", "name2", Set.of("g"), Set.of("all","b","d","y"),
            Instant.parse("2022-09-01T12:10:00.00Z"));
        wbStorage.insertWhiteboard(userId1, wb2, null);
        wbStorage.markFieldLinked(wb2.id(), genLinkedField("g"), wb2.createdAt().plusSeconds(30), null);
        wbStorage.setWhiteboardFinalized(wb2.id(), wb2.createdAt().plusSeconds(60), null);

        final var wb3 = genWhiteboard("id3", "name3", Set.of("g", "h"), Set.of("all","c","d","z"),
            Instant.parse("2022-09-01T12:20:00.00Z"));
        wbStorage.insertWhiteboard(userId1, wb3, null);
        wbStorage.markFieldLinked(wb3.id(), genLinkedField("g"), wb3.createdAt().plusSeconds(30), null);
        wbStorage.markFieldLinked(wb3.id(), genLinkedField("h"), wb3.createdAt().plusSeconds(45), null);
        wbStorage.setWhiteboardFinalized(wb3.id(), wb3.createdAt().plusSeconds(60), null);

        final var wb4 = genWhiteboard("id", "name", Set.of("fun"), Set.of("all", "other"),
            Instant.parse("2022-09-01T12:10:00.00Z"));
        wbStorage.insertWhiteboard(userId2, wb4, null);
        wbStorage.markFieldLinked(wb4.id(), genLinkedField("fun"), wb4.createdAt().plusSeconds(30), null);
        wbStorage.setWhiteboardFinalized(wb4.id(), wb4.createdAt().plusSeconds(60), null);


        Assert.assertEquals(3, wbStorage.listWhiteboards(userId1, null, List.of(), null, null, null).count());
        Assert.assertEquals(1, wbStorage.listWhiteboards(userId2, null, List.of(), null, null, null).count());
        Assert.assertEquals(1, wbStorage.listWhiteboards(userId2, "name", List.of(), null, null, null).count());

        Assert.assertEquals(3, wbStorage.listWhiteboards(userId1, null, List.of("all"), null, null, null).count());
        Assert.assertEquals(1, wbStorage.listWhiteboards(userId1, "name2", List.of("all"), null, null, null).count());
        Assert.assertEquals(2, wbStorage.listWhiteboards(userId1, null, List.of("b"), null, null, null).count());
        Assert.assertEquals(1, wbStorage.listWhiteboards(userId1, null, List.of("x"), null, null, null).count());
        Assert.assertEquals(1, wbStorage.listWhiteboards(userId1, null, List.of("c", "d"), null, null, null).count());
        Assert.assertEquals(0, wbStorage.listWhiteboards(userId1, null, List.of("other"), null, null, null).count());

        Assert.assertEquals(1, wbStorage.listWhiteboards(userId1, null, List.of("b"),
            Instant.parse("2022-09-01T12:10:00.00Z"), null, null).count());
        Assert.assertEquals(1, wbStorage.listWhiteboards(userId1, null, List.of("d"),
            null, Instant.parse("2022-09-01T12:10:00.00Z"), null).count());
        Assert.assertEquals(1, wbStorage.listWhiteboards(userId1, null, List.of(),
            Instant.parse("2022-09-01T12:10:00.00Z"), Instant.parse("2022-09-01T12:10:00.00Z"), null).count());
    }

    private Whiteboard genWhiteboard(String id, String name,
                                     Set<String> fieldNames, Set<String> tags, Instant createdAt)
    {
        return new Whiteboard(id, name, fieldNames, Set.of(), tags,
            new Whiteboard.Storage("s-name", ""), "namespace", Whiteboard.Status.CREATED, createdAt
        );
    }

    private Whiteboard.LinkedField genLinkedField(String name) {
        return new Whiteboard.LinkedField(name, "s-uri-"+name, new DataSchema(SchemeType.plain, "default"));
    }
}
