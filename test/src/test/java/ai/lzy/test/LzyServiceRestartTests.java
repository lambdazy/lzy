//package ai.lzy.test.scenarios;
//
//import ai.lzy.test.context.LzyInProcess;
//import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
//import io.zonky.test.db.postgres.junit.PreparedDbRule;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Rule;
//
//import java.nio.file.Path;
//import java.util.List;
//
//public class LzyServiceRestartTests {
//    @Rule
//    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//    @Rule
//    public PreparedDbRule allocatorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//    @Rule
//    public PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//    @Rule
//    public PreparedDbRule graphExecutorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//    @Rule
//    public PreparedDbRule schedulerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//    @Rule
//    public PreparedDbRule storageDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//    @Rule
//    public PreparedDbRule whiteboardDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//    @Rule
//    public PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//
//    public LzyInProcess lzy = new LzyInProcess(Path.of("..", "test-context", "target"));
//
//    @Before
//    public void setUp() throws Exception {
//        var args = List.of();
//        lzy.before(args);
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        lzy.after();
//    }
//}
