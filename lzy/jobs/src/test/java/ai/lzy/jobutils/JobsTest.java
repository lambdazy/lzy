package ai.lzy.jobutils;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.providers.WaitForOperation;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.grpc.GrpcUtils;
import io.grpc.Status;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import jakarta.inject.Singleton;
import org.junit.*;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

public class JobsTest {
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private JobService service;
    private Provider provider;
    private WaitForOperation waitForOp;

    @Before
    public void setUp() {
        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("jobs", db.getConnectionInfo()));
        service = context.getBean(JobService.class);
        provider = context.getBean(Provider.class);
        this.waitForOp = context.getBean(WaitForOperation.class);
    }

    @After
    public void tearDown() {
        context.close();
    }

    @Test
    public void testSmoke() throws Exception {
        var f = new CompletableFuture<Provider.Data>();
        Provider.onExecute = f::complete;

        var data = new Provider.Data("a", "b");

        service.create(provider, data, null);

        var ret = f.get();

        Assert.assertEquals(data, ret);
    }

    @Test
    public void testOp() throws Exception {
        var operationService = new LocalOperationService("service");
        var server = newGrpcServer("0.0.0.0", 19234, GrpcUtils.NO_AUTH)
            .addService(operationService)
            .build();

        server.start();

        var op = new Operation(
            "opId",
            "test",
            Instant.now(),
            "",
            null,
            null,
            Instant.now(),
            true,
            null,
            null
        );

        try {
            operationService.registerOperation(op);

            var f = new CompletableFuture<WaitForOperation.OperationResult>();

            OpConsumer.onRes = f::complete;

            service.create(waitForOp, new WaitForOperation.OperationDescription(
                "localhost:19234",
                op.id(),
                Duration.ofMillis(100),
                OpConsumer.class.getName(),
                null,
                null,
                null
            ), null);

            var res = f.get();

            Assert.assertEquals(Status.OK.getCode().value(), res.statusCode().intValue());
            Assert.assertEquals("test", res.op().getCreatedBy());

        } finally {
            server.shutdownNow();
            server.awaitTermination();
        }


    }

    @Test
    public void testOpFail() throws Exception {
        var operationService = new LocalOperationService("service");
        var server = newGrpcServer("0.0.0.0", 19234, GrpcUtils.NO_AUTH)
            .addService(operationService)
            .build();

        server.start();

        var op = new Operation(
            "opId",
            "test",
            Instant.now(),
            "",
            null,
            null,
            Instant.now(),
            true,
            null,
            null
        );

        try {

            var f = new CompletableFuture<WaitForOperation.OperationResult>();

            OpConsumer.onRes = f::complete;

            service.create(waitForOp, new WaitForOperation.OperationDescription(
                "localhost:19234",
                op.id(),
                Duration.ofMillis(100),
                OpConsumer.class.getName(),
                null,
                null,
                null
            ), null);

            var res = f.get();

            Assert.assertEquals(Status.NOT_FOUND.getCode().value(), res.statusCode().intValue());

        } finally {
            server.shutdownNow();
            server.awaitTermination();
        }


    }

    @Singleton
    public static class OpConsumer extends WaitForOperation.OperationConsumer {

        public static Consumer<WaitForOperation.OperationResult> onRes = (a) -> {};

        @Override
        protected void execute(WaitForOperation.OperationResult arg) {
            onRes.accept(arg);
        }
    }

}
