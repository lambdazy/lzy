package ai.lzy.test.scenarios;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.graph.GraphExecutorApi;
import ai.lzy.model.Operation;
import ai.lzy.model.Slot;
import ai.lzy.model.data.DataSchema;
import ai.lzy.model.graph.AuxEnv;
import ai.lzy.model.graph.BaseEnv;
import ai.lzy.model.graph.Env;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.allocator.AllocatorImpl;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.test.LzyServerTestContext;
import ai.lzy.test.impl.Utils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.ChannelManager.ChannelCreateRequest;
import ai.lzy.v1.Channels.ChannelSpec;
import ai.lzy.v1.Channels.DirectChannelType;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Operations.DataScheme;
import ai.lzy.v1.SchedulerApi.KillAllRequest;
import ai.lzy.v1.SchedulerGrpc;
import ai.lzy.v1.graph.GraphExecutorApi.*;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static ai.lzy.test.impl.ChannelManagerThreadContext.Config.*;

public class SchedulerTest extends LocalScenario {
    static final Logger LOG = LogManager.getLogger(SchedulerTest.class);

    static final int allocPort = FreePortFinder.find(10000, 11000);
    static final int schedulerPort = FreePortFinder.find(11000, 12000);
    static final int graphExecutorPort = FreePortFinder.find(12000, 13000);
    static final Map<String, Object> opt = new HashMap<>();
    static final Map<String, Object> options1 = Map.of(
        "allocator.address", "localhost:" + allocPort,
        "allocator.mock-mk8s.enabled", "true",
        "allocator.thread-allocator.enabled", "true",
        "allocator.thread-allocator.vm-jar-file", "../servant/target/servant-1.0-SNAPSHOT.jar",
        "allocator.thread-allocator.vm-class-name", "ai.lzy.servant.agents.Worker",
        "scheduler.scheduler-address", "localhost:" + schedulerPort,
        "scheduler.port", schedulerPort,
        "scheduler.allocator-address", "localhost:" + allocPort,
        "scheduler.channel-manager-address", "localhost:" + CHANNEL_MANAGER_PORT,
        "graph-executor.port", graphExecutorPort
    );
    static final Map<String, Object> options2 = Map.of(
        "graph-executor.executors-count", 1,
        "graph-executor.scheduler.host", "localhost",
        "graph-executor.scheduler.port", schedulerPort
    );

    static {
        opt.putAll(options1);
        opt.putAll(options2);
        opt.putAll(Utils.createModuleDatabase("iam"));
        opt.putAll(Utils.createModuleDatabase("allocator"));
        opt.putAll(Utils.createModuleDatabase("scheduler"));
        opt.putAll(Utils.createModuleDatabase("graph-executor"));
    }

    static final ApplicationContext context = ApplicationContext.run(opt);
    private static final AllocatorMain alloc = context.getBean(AllocatorMain.class);
    private static final SchedulerApi scheduler = context.getBean(SchedulerApi.class);
    private static final GraphExecutorApi graphExecutor = context.getBean(GraphExecutorApi.class);

    static {
        try {
            AllocatorImpl.randomServantPorts.set(true);
            alloc.start();
            graphExecutor.start();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static final SchedulerGrpc.SchedulerBlockingStub stub;
    private static final GraphExecutorGrpc.GraphExecutorBlockingStub geStub;
    private static final LzyChannelManagerGrpc.LzyChannelManagerBlockingStub cmStub;

    static {
        final var channel = ChannelBuilder.forAddress("localhost:" + schedulerPort)
            .usePlaintext()
            .enableRetry(SchedulerGrpc.SERVICE_NAME)
            .build();
        final var config = context.getBean(ServiceConfig.class);
        final var credentials = config.getIam().createCredentials();
        stub = SchedulerGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        final var chan = ChannelBuilder.forAddress("localhost:" + graphExecutorPort)
            .usePlaintext()
            .enableRetry(GraphExecutorGrpc.SERVICE_NAME)
            .build();
        geStub = GraphExecutorGrpc.newBlockingStub(chan).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        final var ch = ChannelBuilder.forAddress("localhost:" + CHANNEL_MANAGER_PORT)
            .usePlaintext()
            .enableRetry(LzyChannelManagerGrpc.SERVICE_NAME)
            .build();
        cmStub = LzyChannelManagerGrpc.newBlockingStub(ch).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
    }

    @Before
    public void setUp() {
        super.setUp(LzyServerTestContext.LocalServantAllocatorType.THREAD_ALLOCATOR, false);
    }

    @After
    public void after() throws SQLException {
        stub.killAll(KillAllRequest.newBuilder()
            .setWorkflowName("wf")
            .setIssue("test")
            .build());
        scheduler.awaitWorkflowTermination("wf");
        alloc.destroyAll();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        alloc.stop();
        alloc.awaitTermination();
        scheduler.close();
        scheduler.awaitTermination();
        graphExecutor.close();
        graphExecutor.awaitTermination();
    }

    @Test
    public void testGE() throws InterruptedException {
        final var ch1 = buildChannel("1");
        final var ch2 = buildChannel("2");
        final var ch3 = buildChannel("3");
        final var ch4 = buildChannel("4");

        final var t1 = buildTask("1", "echo 41 > $LZY_MOUNT/o1 && echo 42 > $LZY_MOUNT/o2",
            List.of(), List.of("/o1", "/o2"), Map.of("/o1", ch1, "/o2", ch2));

        final var t2 = buildTask("2", "cat $LZY_MOUNT/i1 > $LZY_MOUNT/o3", List.of("/i1"),
            List.of("/o3"), Map.of("/i1", ch1, "/o3", ch3));

        final var t3 = buildTask("3", "cat $LZY_MOUNT/i2 > $LZY_MOUNT/o4", List.of("/i2"),
            List.of("/o4"), Map.of("/i2", ch2, "/o4", ch4));

        final var t4 = buildTask("4", "cat $LZY_MOUNT/i3 >> /tmp/res.txt && cat $LZY_MOUNT/i4 >> /tmp/res.txt",
            List.of("/i3", "/i4"), List.of(), Map.of("/i3", ch3, "/i4", ch4));

        final var g1 = geStub.execute(GraphExecuteRequest.newBuilder()
            .setWorkflowId("wf_id")
            .setWorkflowName("wf")
            .addChannels(ChannelDesc.newBuilder()
                .setId(ch1)
                .setDirect(ChannelDesc.DirectChannel.newBuilder().build())
                .build())
            .addChannels(ChannelDesc.newBuilder()
                .setId(ch2)
                .setDirect(ChannelDesc.DirectChannel.newBuilder().build())
                .build())
            .addChannels(ChannelDesc.newBuilder()
                .setId(ch3)
                .setDirect(ChannelDesc.DirectChannel.newBuilder().build())
                .build())
            .addChannels(ChannelDesc.newBuilder()
                .setId(ch4)
                .setDirect(ChannelDesc.DirectChannel.newBuilder().build())
                .build())
            .addAllTasks(List.of(t1, t2, t3, t4))
            .build()).getStatus().getGraphId();

        GraphExecutionStatus status;

        do {
            Thread.sleep(1000);
            status = geStub.status(GraphStatusRequest.newBuilder()
                .setGraphId(g1)
                .setWorkflowId("wf_id")
                .build()).getStatus();

            LOG.info("Exec status: {}", JsonUtils.printRequest(status));

        } while (!status.hasCompleted());
    }

    @NotNull
    private String buildChannel(String value) {
        return cmStub.create(ChannelCreateRequest.newBuilder()
            .setWorkflowId("wf_id")
            .setChannelSpec(ChannelSpec.newBuilder()
                .setChannelName(value)
                .setDirect(DirectChannelType.newBuilder().build())
                    .setContentType(DataScheme.newBuilder()
                        .setSchemeType(Operations.SchemeType.plain)
                        .setType("text")
                        .build())
                .build())
            .build()).getChannelId();
    }

    private ai.lzy.v1.graph.GraphExecutorApi.TaskDesc buildTask(String id, String command, List<String> inputs,
                                                                List<String> outputs, Map<String, String> bindings)
    {
        final var op = new Operation(
            buildEnv(),
            new Operation.Requirements("s", "A"),
            command,
            Stream.concat(
                inputs.stream().map(t -> buildSlot(t, Slot.Direction.INPUT)),
                outputs.stream().map(t -> buildSlot(t, Slot.Direction.OUTPUT))
            ).toList(),
            "", "", null, null
        ).toProto();

        return ai.lzy.v1.graph.GraphExecutorApi.TaskDesc.newBuilder()
            .setOperation(op)
            .setId(id)
            .addAllSlotAssignments(
                bindings.entrySet()
                    .stream()
                    .map(e -> SlotToChannelAssignment.newBuilder()
                        .setSlotName(e.getKey())
                        .setChannelId(e.getValue())
                        .build()
                        )
                    .toList()
            )
            .build();
    }

    private Env buildEnv() {
        return new Env() {
            @Override
            public BaseEnv baseEnv() {
                return null;
            }

            @Override
            public AuxEnv auxEnv() {
                return null;
            }
        };
    }

    private Slot buildSlot(String name, Slot.Direction direction) {
        return new Slot() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Media media() {
                return Media.FILE;
            }

            @Override
            public Direction direction() {
                return direction;
            }

            @Override
            public DataSchema contentType() {
                return DataSchema.plain;
            }
        };
    }

}
