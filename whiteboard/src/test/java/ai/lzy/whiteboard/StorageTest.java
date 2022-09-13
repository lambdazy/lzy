package ai.lzy.whiteboard;

import ai.lzy.model.data.DataSchema;
import ai.lzy.model.data.SchemeType;
import ai.lzy.model.db.NotFoundException;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.whiteboard.model.Field;
import ai.lzy.whiteboard.model.LinkedField;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import ai.lzy.whiteboard.storage.WhiteboardStorage;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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

        final var wb = new Whiteboard("id1", "wb-name-1", Map.of(
            "f1", new Field("f1", Field.Status.CREATED),
            "f2", new Field("f2", Field.Status.CREATED),
            "f3", new Field("f3", Field.Status.CREATED)
        ), Set.of(), new Whiteboard.Storage("s-name", ""), "namespace", Whiteboard.Status.CREATED, timestampNow());

        wbStorage.insertWhiteboard(userId, wb, null);
        Assert.assertEquals(wb, wbStorage.getWhiteboard(wb.id(), null));

        final var wbWithDefaults = new Whiteboard("id2", "wb-name-2", Map.of(
            "f1", new Field("f1", Field.Status.CREATED),
            "f2", genLinkedField("f2", Field.Status.CREATED),
            "f3", new Field("f3", Field.Status.CREATED),
            "f4", genLinkedField("f4", Field.Status.CREATED)
        ), Set.of(), new Whiteboard.Storage("s-name", ""), "namespace", Whiteboard.Status.CREATED, timestampNow());

        wbStorage.insertWhiteboard(userId, wbWithDefaults, null);
        Assert.assertEquals(wbWithDefaults, wbStorage.getWhiteboard(wbWithDefaults.id(), null));

        final var wbTagged = new Whiteboard("id3", "wb-name-1",
            Map.of("f1", new Field("f1", Field.Status.CREATED)), Set.of("lol", "kek", "cheburek"),
            new Whiteboard.Storage("s-name", "My super secret storage."), "namespace",
            Whiteboard.Status.CREATED, timestampNow()
        );

        wbStorage.insertWhiteboard(userId, wbTagged, null);
        Assert.assertEquals(wbTagged, wbStorage.getWhiteboard(wbTagged.id(), null));
    }

    @Test
    public void finalizeWhiteboardAndFields() throws SQLException {
        final var userId = "uid-42";

        Whiteboard wb = genWhiteboard("id", "wb-name", Set.of("f1", "f2"), Set.of("t1"), timestampNow());
        wbStorage.insertWhiteboard(userId, wb, null);

        final var f0 = genLinkedFinalizedField("f0");
        final var f1 = genLinkedFinalizedField("f1");
        final var f2 = new Field("f2", Field.Status.FINALIZED);

        wbStorage.updateField(wb.id(), f1, timestampNow(), null);
        wb = wbStorage.getWhiteboard(wb.id(), null);
        Assert.assertEquals(1, wb.unlinkedFields().size());
        Assert.assertEquals(1, wb.linkedFields().size());
        Assert.assertEquals(Field.Status.FINALIZED, wb.getField(f1.name()).status());
        Assert.assertEquals(Field.Status.CREATED, wb.getField(f2.name()).status());

        try {
            wbStorage.updateField(wb.id(), f0, timestampNow(), null);
            Assert.fail("Field doesn't exist, but has successfully linked");
        } catch (NotFoundException e) {
            // ignored
        } catch (Exception e) {
            Assert.fail("Unexpected exception");
        }

        wbStorage.finalizeWhiteboard(wb.id(), timestampNow(), null);
        final var expectedFinalizedWb = new Whiteboard(
            wb.id(), wb.name(), Map.of(f1.name(), f1, f2.name(), f2), wb.tags(),
            wb.storage(), wb.namespace(), Whiteboard.Status.FINALIZED, wb.createdAt());

        wb = wbStorage.getWhiteboard(wb.id(), null);
        Assert.assertEquals(expectedFinalizedWb, wb);
        Assert.assertEquals(1, wb.unlinkedFields().size());
        Assert.assertEquals(1, wb.linkedFields().size());
        Assert.assertEquals(Field.Status.FINALIZED, wb.getField(f1.name()).status());
        Assert.assertEquals(Field.Status.FINALIZED, wb.getField(f2.name()).status());
    }

    @Test
    public void listWhiteboards() throws SQLException {
        final var userId1 = "uid1";
        final var userId2 = "uid2";

        final var wb1 = genWhiteboard("id1", "name1", Set.of("f"), Set.of("all","b","c","x"),
            Instant.parse("2022-09-01T12:00:00.00Z"));
        wbStorage.insertWhiteboard(userId1, wb1, null);
        wbStorage.updateField(wb1.id(), genLinkedFinalizedField("f"), wb1.createdAt().plusSeconds(30), null);
        wbStorage.finalizeWhiteboard(wb1.id(), wb1.createdAt().plusSeconds(60), null);

        final var wb2 = genWhiteboard("id2", "name2", Set.of("g"), Set.of("all","b","d","y"),
            Instant.parse("2022-09-01T12:10:00.00Z"));
        wbStorage.insertWhiteboard(userId1, wb2, null);
        wbStorage.updateField(wb2.id(), genLinkedFinalizedField("g"), wb2.createdAt().plusSeconds(30), null);
        wbStorage.finalizeWhiteboard(wb2.id(), wb2.createdAt().plusSeconds(60), null);

        final var wb3 = genWhiteboard("id3", "name3", Set.of("g", "h"), Set.of("all","c","d","z"),
            Instant.parse("2022-09-01T12:20:00.00Z"));
        wbStorage.insertWhiteboard(userId1, wb3, null);
        wbStorage.updateField(wb3.id(), genLinkedFinalizedField("g"), wb3.createdAt().plusSeconds(30), null);
        wbStorage.updateField(wb3.id(), genLinkedFinalizedField("h"), wb3.createdAt().plusSeconds(45), null);
        wbStorage.finalizeWhiteboard(wb3.id(), wb3.createdAt().plusSeconds(60), null);

        final var wb4 = genWhiteboard("id", "name", Set.of("fun"), Set.of("all", "other"),
            Instant.parse("2022-09-01T12:10:00.00Z"));
        wbStorage.insertWhiteboard(userId2, wb4, null);
        wbStorage.updateField(wb4.id(), genLinkedFinalizedField("fun"), wb4.createdAt().plusSeconds(30), null);
        wbStorage.finalizeWhiteboard(wb4.id(), wb4.createdAt().plusSeconds(60), null);


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
        final var whiteboardFields = fieldNames.stream()
            .map(fieldName -> new Field(fieldName, Field.Status.CREATED))
            .collect(Collectors.toMap(Field::name, f -> f));
        return new Whiteboard(id, name, whiteboardFields, tags,
            new Whiteboard.Storage("s-name", ""), "namespace", Whiteboard.Status.CREATED, createdAt);
    }

    private static Instant timestampNow() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private LinkedField genLinkedFinalizedField(String name) {
        return genLinkedField(name, Field.Status.FINALIZED);
    }

    private LinkedField genLinkedField(String name, Field.Status status) {
        return new LinkedField(name, status,
            "s-uri-" + name, new DataSchema(SchemeType.plain, "default"));
    }
}
