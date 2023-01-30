package ai.lzy.whiteboard;

import ai.lzy.model.DataScheme;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.whiteboard.model.Field;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import ai.lzy.whiteboard.storage.WhiteboardStorage;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.net.URI;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class StorageTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });

    private WhiteboardStorage wbStorage;
    private ApplicationContext context;

    @Before
    public void setUp() {
        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("whiteboard", db.getConnectionInfo()));
        wbStorage = context.getBean(WhiteboardStorage.class);
    }

    @After
    public void tearDown() {
        context.getBean(WhiteboardDataSource.class).setOnClose(DatabaseTestUtils::cleanup);
        context.stop();
    }

    @Test
    public void insertAndGetWhiteboard() throws SQLException {
        final var userId = "uid-42";

        final var wb = new Whiteboard("id1", "wb-name-1", Map.of(
            "f1", new Field("f1", DataScheme.PLAIN),
            "f2", new Field("f2", DataScheme.PLAIN),
            "f3", new Field("f3", DataScheme.PLAIN)
        ), Set.of(), new Whiteboard.Storage("s-name", "", URI.create("")), "namespace", Whiteboard.Status.CREATED,
            timestampNow());

        wbStorage.registerWhiteboard(userId, wb, Instant.now(), null);
        Assert.assertEquals(wb, wbStorage.getWhiteboard(wb.id(), null));

        final var wbTagged = new Whiteboard("id3", "wb-name-1",
            Map.of("f1", new Field("f1", DataScheme.PLAIN)), Set.of("tag1", "tag2", "tag3"),
            new Whiteboard.Storage("s-name", "My super secret storage.", URI.create("")), "namespace",
            Whiteboard.Status.CREATED, timestampNow()
        );

        wbStorage.registerWhiteboard(userId, wbTagged, Instant.now(), null);
        Assert.assertEquals(wbTagged, wbStorage.getWhiteboard(wbTagged.id(), null));
    }

    @Test
    public void deleteWhiteboard() throws SQLException {
        final var userId = "uid-42";

        Whiteboard wb =
            genWhiteboard(UUID.randomUUID().toString(), "wb-name", Set.of("f1", "f2"), Set.of("t1"), timestampNow());
        wbStorage.registerWhiteboard(userId, wb, Instant.now(), null);
        Assert.assertEquals(wb, wbStorage.getWhiteboard(wb.id(), null));

        wbStorage.deleteWhiteboard(wb.id(), null);
        Assert.assertThrows(NotFoundException.class, () -> wbStorage.getWhiteboard(wb.id(), null));
        Assert.assertThrows(NotFoundException.class, () -> wbStorage.deleteWhiteboard(wb.id(), null));
    }

    @Test
    public void listWhiteboards() throws SQLException {
        final var userId1 = "uid1";
        final var userId2 = "uid2";

        final var wb1 = genWhiteboard("id1", "name1", Set.of("f"), Set.of("all", "b", "c", "x"),
            Instant.parse("2022-09-01T12:00:00.00Z"));
        wbStorage.registerWhiteboard(userId1, wb1, Instant.now(), null);

        final var wb2 = genWhiteboard("id2", "name2", Set.of("g"), Set.of("all", "b", "d", "y"),
            Instant.parse("2022-09-01T12:10:00.00Z"));
        wbStorage.registerWhiteboard(userId1, wb2, Instant.now(), null);

        final var wb3 = genWhiteboard("id3", "name3", Set.of("g", "h"), Set.of("all", "c", "d", "z"),
            Instant.parse("2022-09-01T12:20:00.00Z"));
        wbStorage.registerWhiteboard(userId1, wb3, Instant.now(), null);

        final var wb4 = genWhiteboard("id", "name", Set.of("fun"), Set.of("all", "other"),
            Instant.parse("2022-09-01T12:10:00.00Z"));
        wbStorage.registerWhiteboard(userId2, wb4, Instant.now(), null);

        final var wb5 = genWhiteboard("id0", "name0", Set.of("fun"), Set.of(),
            Instant.parse("2022-09-01T12:10:00.00Z"));
        wbStorage.registerWhiteboard(userId2, wb5, Instant.now(), null);


        Assert.assertEquals(3, wbStorage.listWhiteboards(userId1, null, List.of(), null, null, null).count());
        Assert.assertEquals(2, wbStorage.listWhiteboards(userId2, null, List.of(), null, null, null).count());
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

    private Whiteboard genWhiteboard(String id, String name, Set<String> fieldNames, Set<String> tags,
                                     Instant createdAt)
    {
        final var whiteboardFields = fieldNames.stream()
            .map(fieldName -> new Field(fieldName, DataScheme.PLAIN))
            .collect(Collectors.toMap(Field::name, f -> f));
        return new Whiteboard(id, name, whiteboardFields, tags,
            new Whiteboard.Storage("s-name", "", URI.create("")), "namespace", Whiteboard.Status.CREATED, createdAt);
    }

    private static Instant timestampNow() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }
}
