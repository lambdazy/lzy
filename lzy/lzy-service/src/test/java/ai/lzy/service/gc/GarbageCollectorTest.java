package ai.lzy.service.gc;

import ai.lzy.allocator.test.AllocatorProxy;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.service.App;
import ai.lzy.service.BaseTest;
import ai.lzy.service.LzyService;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

@Ignore
public class GarbageCollectorTest extends BaseTest {
    private final List<Server> lzyServers = new ArrayList<>();
    private final List<ApplicationContext> lzyContexts = new ArrayList<>();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);

    @Test
    public void testFailOnExecuteWith1Instance() throws IOException, InterruptedException {
        testFailOnExecute(1);
    }

    @Test
    public void testFailOnExecuteWith2Instances() throws IOException, InterruptedException {
        testFailOnExecute(2);
    }

    @Test
    public void testFailOnExecuteWithManyInstances() throws IOException, InterruptedException {
        testFailOnExecute(10);
    }

    private void testFailOnExecute(int numOfInstances) throws InterruptedException, IOException {
        for (int i = 1; i < numOfInstances; i++) {
            startAnotherInstance();
        }

        final BlockingQueue<String> alloc = new LinkedBlockingQueue<>();
        final BlockingQueue<String> free = new LinkedBlockingQueue<>();
        final BlockingQueue<String> createSession = new LinkedBlockingQueue<>();
        final BlockingQueue<String> deleteSession = new LinkedBlockingQueue<>();

        AllocatorProxy allocatorProxy = allocatorTestContext.getContext().getBean(AllocatorProxy.class);
        allocatorProxy.setOnAllocate(() -> alloc.add(""));
        allocatorProxy.setOnFree(() -> free.add(""));
        allocatorProxy.setOnCreateSession(() -> createSession.add(""));
        allocatorProxy.setOnDeleteSession(() -> deleteSession.add(""));


        var workflowName = "workflow_" + numOfInstances;
        var executionId = authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        createSession.take();
        alloc.take();

        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .build();

        Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient
                .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> "idempotency-key"))
                .executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                    .setExecutionId(executionId)
                    .setGraph(graph)
                    .build()));

        Thread.sleep(2000);

        free.take();
        deleteSession.take();
    }

    @Test
    public void testFailOnAllocWith1Instance() throws IOException, InterruptedException {
        testFailOnAlloc(1);
    }

    @Test
    public void testFailOnAllocWith2Instances() throws IOException, InterruptedException {
        testFailOnAlloc(2);
    }

    @Test
    public void testFailOnAllocWithManyInstances() throws IOException, InterruptedException {
        testFailOnAlloc(10);
    }

    private void testFailOnAlloc(int numOfInstances) throws InterruptedException, IOException {
        for (int i = 1; i < numOfInstances; i++) {
            startAnotherInstance();
        }

        final BlockingQueue<String> alloc = new LinkedBlockingQueue<>();
        final BlockingQueue<String> free = new LinkedBlockingQueue<>();
        final BlockingQueue<String> createSession = new LinkedBlockingQueue<>();
        final BlockingQueue<String> deleteSession = new LinkedBlockingQueue<>();

        AllocatorProxy allocatorProxy = allocatorTestContext.getContext().getBean(AllocatorProxy.class);
        allocatorProxy.setOnAllocate(() -> {
            alloc.add("");
            throw new RuntimeException();
        });
        allocatorProxy.setOnFree(() -> free.add(""));
        allocatorProxy.setOnCreateSession(() -> createSession.add(""));
        allocatorProxy.setOnDeleteSession(() -> deleteSession.add(""));


        var workflowName = "workflow_" + numOfInstances;
        Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName).build()).getExecutionId());

        createSession.take();
        alloc.take();

        deleteSession.take();

        Assert.assertTrue(free.isEmpty());
    }

    @Test
    public void testFailOnCreateSessionWith1Instance() throws IOException, InterruptedException {
        testFailOnCreateSession(1);
    }

    @Test
    public void testFailOnCreateSessionWith2Instances() throws IOException, InterruptedException {
        testFailOnCreateSession(2);
    }

    @Test
    public void testFailOnCreateSessionWithManyInstances() throws IOException, InterruptedException {
        testFailOnCreateSession(10);
    }

    private void testFailOnCreateSession(int numOfInstances) throws InterruptedException, IOException {
        for (int i = 1; i < numOfInstances; i++) {
            startAnotherInstance();
        }

        final BlockingQueue<String> alloc = new LinkedBlockingQueue<>();
        final BlockingQueue<String> free = new LinkedBlockingQueue<>();
        final BlockingQueue<String> createSession = new LinkedBlockingQueue<>();
        final BlockingQueue<String> deleteSession = new LinkedBlockingQueue<>();

        AllocatorProxy allocatorProxy = allocatorTestContext.getContext().getBean(AllocatorProxy.class);
        allocatorProxy.setOnAllocate(() -> alloc.add(""));
        allocatorProxy.setOnFree(() -> free.add(""));
        allocatorProxy.setOnCreateSession(() -> {
            createSession.add("");
            throw new RuntimeException();
        });
        allocatorProxy.setOnDeleteSession(() -> deleteSession.add(""));

        var workflowName = "workflow_" + numOfInstances;

        Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName).build()).getExecutionId());
        createSession.take();

        Assert.assertTrue(deleteSession.isEmpty());
        Assert.assertTrue(free.isEmpty());
    }

    @After
    public void tearDown() throws SQLException, InterruptedException, DaoException {
        super.tearDown();
        lzyServers.forEach(s -> {
            try {
                s.shutdownNow();
                s.awaitTermination();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        lzyServers.clear();
        lzyContexts.forEach(ApplicationContext::stop);
        lzyContexts.clear();
    }

    private void startAnotherInstance() throws IOException {
        var lzyDbConfig = preparePostgresConfig("lzy-service", lzyServiceDb.getConnectionInfo());
        var context2 = ApplicationContext.run(PropertySource.of(lzyDbConfig));
        var port = FreePortFinder.find(8000, 9000);
        var workflowAddress = HostAndPort.fromString("localhost:" + port);
        var lzyServer = App.createServer(workflowAddress, authInterceptor, context2.getBean(LzyService.class));

        lzyServers.add(lzyServer);
        lzyContexts.add(context2);

        lzyServer.start();
    }
}
