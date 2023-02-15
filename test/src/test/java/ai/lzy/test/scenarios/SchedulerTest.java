package ai.lzy.test.scenarios;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.impl.v2.AllocatorContext;
import ai.lzy.test.impl.v2.ChannelManagerContext;
import ai.lzy.test.impl.v2.GraphExecutorContext;
import ai.lzy.test.impl.v2.IamContext;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutor.ChannelDesc;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecuteRequest;
import ai.lzy.v1.graph.GraphExecutorApi.GraphStatusRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
import static ai.lzy.v1.common.LMS.Slot.Direction.INPUT;
import static ai.lzy.v1.common.LMS.Slot.Direction.OUTPUT;

public class SchedulerTest {
    static final Logger LOG = LogManager.getLogger(SchedulerTest.class);

    @Rule
    public final ApplicationContextRule ctx = new ApplicationContextRule();

    @Rule
    public final ContextRule<IamContext> iam = new ContextRule<>(ctx, IamContext.class);

    @Rule
    public final ContextRule<GraphExecutorContext> graphExecutor = new ContextRule<>(ctx, GraphExecutorContext.class);

    @Rule
    public final ContextRule<ChannelManagerContext> channelManager
        = new ContextRule<>(ctx, ChannelManagerContext.class);

    @Test(timeout = 120_000)
    public void testGE() throws Exception {
        var cacheLimits = ctx.getCtx().getBean(AllocatorContext.WorkerAllocatorContext.class).context()
            .getBean(ServiceConfig.CacheLimits.class);
        cacheLimits.setUserLimit(Integer.MAX_VALUE);
        cacheLimits.setSessionLimit(Integer.MAX_VALUE);
        cacheLimits.setSessionPoolLimit(null);
        cacheLimits.setAnySessionPoolLimit(Integer.MAX_VALUE);

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

        final var g1 = graphExecutor.context().stub().execute(GraphExecuteRequest.newBuilder()
            .setWorkflowId("wf_id")
            .setWorkflowName("wf")
            .setUserId("Semjon.Semjonych")
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

        GraphExecutor.GraphExecutionStatus status;

        do {
            //noinspection BusyWait
            Thread.sleep(1000);
            status = graphExecutor.context().stub().status(GraphStatusRequest.newBuilder()
                .setGraphId(g1)
                .setWorkflowId("wf_id")
                .build()).getStatus();

            LOG.info("Exec status: {}", JsonUtils.printRequest(status));

        } while (!status.hasCompleted() && !status.hasFailed());

        Assert.assertTrue(status.hasCompleted());
    }

    @NotNull
    private String buildChannel(String channelName) {
        final var client = channelManager.context().privateClient();
        final var response = client.create(makeCreateChannelCommand("Semjon.Semjonych", "wf", "wf_id", channelName));
        return response.getChannelId();
    }

    private GraphExecutor.TaskDesc buildTask(String id, String command, List<String> inputs,
                                             List<String> outputs, Map<String, String> bindings)
    {
        final var op = LMO.Operation.newBuilder()
            .setEnv(
                LME.EnvSpec.newBuilder()
                    .setProcessEnv(LME.ProcessEnv.newBuilder().build())
                    .build()
            )
            .setRequirements(LMO.Requirements.newBuilder()
                .setPoolLabel("s")
                .setZone("A")
                .build())
            .setCommand(command)
            .addAllSlots(Stream.concat(
                inputs.stream().map(t -> buildSlot(t, INPUT)),
                outputs.stream().map(t -> buildSlot(t, OUTPUT))
            ).toList())
            .build();

        return GraphExecutor.TaskDesc.newBuilder()
            .setOperation(op)
            .setId(id)
            .addAllSlotAssignments(
                bindings.entrySet()
                    .stream()
                    .map(e -> GraphExecutor.SlotToChannelAssignment.newBuilder()
                        .setSlotName(e.getKey())
                        .setChannelId(e.getValue())
                        .build()
                        )
                    .toList()
            )
            .build();
    }

    public static LMS.Slot buildSlot(String name, LMS.Slot.Direction direction) {

        return LMS.Slot.newBuilder()
            .setContentType(LMD.DataScheme.newBuilder()
                .setSchemeFormat("plain")
                .setDataFormat("text")
                .build())
            .setMedia(LMS.Slot.Media.FILE)
            .setName(name)
            .setDirection(direction)
            .build();
    }

}
