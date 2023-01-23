package ai.lzy.jobutils;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.db.JobsOperationDao;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationsService;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.grpc.GrpcUtils;
import com.google.rpc.Status;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

public class JobsTest {
    private static final Logger LOG = LogManager.getLogger(JobsTest.class);
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private JobService service;
    private Provider provider;
    private JobsOperationDao opDao;

    @Before
    public void setUp() {
        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("jobs", db.getConnectionInfo()));
        service = context.getBean(JobService.class);
        provider = context.getBean(Provider.class);
        opDao = context.getBean(JobsOperationDao.class);

    }

    @After
    public void tearDown() {
        service.stop();
        context.close();
    }

    @Test
    public void testSmoke() throws Exception {
        var f = new CompletableFuture<Provider.Data>();
        Provider.onExecute = f::complete;

        var data = new Provider.Data("a", "b");
        provider.schedule(data, null);

        var ret = f.get();

        Assert.assertEquals(data, ret);
    }

    @Test
    public void testOp() throws Exception {
        var operationService = new OperationsService(opDao);
        var server = newGrpcServer("0.0.0.0", 19234, GrpcUtils.NO_AUTH)
            .addService(operationService)
            .build();

        server.start();

        var op = new Operation(
            "opId",
            "test",
            Instant.now(),
            "",
            Instant.now().plus(Duration.ofDays(10)),
            null,
            null,
            Instant.now(),
            false,
            null,
            null
        );

        try {
            opDao.create(op, null);

            var f1 = new CompletableFuture<Provider.Data>();

            A.onExecute = (d) -> {
                f1.complete(d);
                return new Provider.Data("1", "1");
            };

            var f2 = new CompletableFuture<Provider.Data>();

            B.onExecute = (d) -> {
                f2.complete(d);
                return new Provider.Data("2", "2");
            };

            var a = context.getBean(A.class);

            a.schedule(op.id(), new Provider.Data("0", "0"), null, null);

            var res = f1.get();

            Assert.assertEquals("0", res.a());

            res = f2.get();

            Assert.assertEquals("1", res.a());

        } finally {
            server.shutdownNow();
            server.awaitTermination();
        }
    }

    @Test
    public void testFailOp() throws Exception {
        var operationService = new OperationsService(opDao);
        var server = newGrpcServer("0.0.0.0", 19234, GrpcUtils.NO_AUTH)
                .addService(operationService)
                .build();

        server.start();

        var op = new Operation(
                "opId",
                "test",
                Instant.now(),
                "",
                Instant.now().plus(Duration.ofDays(10)),
                null,
                null,
                Instant.now(),
                false,
                null,
                null
        );

        try {
            opDao.create(op, null);

            var f1 = new CompletableFuture<Provider.Data>();

            A.onClear = (d) -> {
                f1.complete(d);
                return new Provider.Data("2", "2");
            };

            var f2 = new CompletableFuture<Provider.Data>();

            B.onClear = (d) -> {
                f2.complete(d);
                return new Provider.Data("1", "1");
            };

            A.onExecute = (d) -> {
                try {
                    opDao.failOperation(op.id(), Status.newBuilder()
                        .setCode(io.grpc.Status.Code.INTERNAL.value())
                        .build(), null, LOG);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return d;
            };

            var a = context.getBean(A.class);

            a.schedule(op.id(), new Provider.Data("0", "0"), null, null);

            var res = f2.get();

            Assert.assertEquals("0", res.a());

            res = f1.get();

            Assert.assertEquals("1", res.a());

        } finally {
            server.shutdownNow();
            server.awaitTermination();
        }
    }

}
